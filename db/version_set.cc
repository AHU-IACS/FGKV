// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

// ///////////////////////////////////////
// #include "util/fg_stats.h"
// // extern FG_Stats fg_stats;
// ///////////////////////////////////////

namespace leveldb {

///////////////////////////////////////
extern std::vector<BlockInfoManager*> scan_b_;
///////////////////////////////////////

static int TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// static double MaxBytesForLevel(const Options* options, int level) {
//   // Note: the result for level zero is not really used since we set
//   // the level-0 compaction threshold based on number of files.

//   // Result for both level-0 and level-1
//   double result = 10. * 1048576.0;
//   while (level > 1) {
//     ///////////////////////////////////////
//     result *= 10;
//     //result *= 12.5;
//     ///////////////////////////////////////
//     level--;
//   }
//   return result;
// }

///////////////////////////////////////
static double MaxBytesForLevel(unsigned level) {
  assert(level < leveldb::config::kNumLevels);
  /*static const double bytes[] = {32 * 1048576.0,
                                 32 * 1048576.0,
                                 256 * 1048576.0,
                                 2048 * 1048576.0,
                                 16384 * 1048576.0,
                                 131072 * 1048576.0,
                                 1048576 * 1048576.0};*/
  static const double bytes[] = {32 * 1048576.0,
                                 32 * 1048576.0,
                                 320 * 1048576.0,
                                 3200 * 1048576.0,
                                 32000 * 1048576.0,
                                 320000 * 1048576.0,
                                 3200000 * 1048576.0};
  return bytes[level];
}
///////////////////////////////////////

// static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
//   // We could vary per level to reduce number of files?
//   return TargetFileSize(options);
// }

///////////////////////////////////////
static uint64_t OriginalClusterSizeForLevel(unsigned level) {
  assert(level < leveldb::config::kNumLevels);
  // static const uint64_t bytes[] = {8 * 1048576,
  //                                  8 * 1048576,
  //                                  8 * 1048576,
  //                                  16 * 1048576,
  //                                  32 * 1048576,
  //                                  64 * 1048576,
  //                                  128 * 1048576};
  static const uint64_t bytes[] = {8 * 1048576,
                                   16 * 1048576,
                                   16 * 1048576,
                                   32 * 1048576,
                                   64 * 1048576,
                                   128 * 1048576,
                                   256 * 1048576};
  return bytes[level];
}
///////////////////////////////////////

///////////////////////////////////////
static uint64_t MaxClusterSizeForLevel(unsigned level) {
  assert(level < leveldb::config::kNumLevels);
  // static const uint64_t bytes[] = {8 * 1048576,
  //                                  8 * 1048576,
  //                                  32 * 1048576,
  //                                  64 * 1048576,
  //                                  128 * 1048576,
  //                                  256 * 1048576,
  //                                  512 * 1048576};
  static const uint64_t bytes[] = {8 * 1048576,
                                   16 * 1048576,
                                   128 * 1048576,
                                   256 * 1048576,
                                   512 * 1048576,
                                   1024 * 1048576,
                                   2048 * 1048576};
  return bytes[level];
}
///////////////////////////////////////

// ///////////////////////////////////////
// static uint64_t MaxPatchSizeForLevel(unsigned level) {
//   assert(level < leveldb::config::kNumLevels);
//   static const uint64_t bytes[] = {64 * 1048576,
//                                    64 * 1048576,
//                                    64 * 1048576,
//                                    64 * 1048576,
//                                    128 * 1048576,
//                                    256 * 1048576,
//                                    512 * 1048576};
//   return bytes[level];
// }
// ///////////////////////////////////////

///////////////////////////////////////
static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  // for (size_t i = 0; i < files.size(); i++) {
  //   for(int j = 0; j < files[i]->child_file.size(); j++){
  //     sum += files[i]->child_file[j].file_size;
  //   }
  // }
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}
///////////////////////////////////////

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

///////////////////////////////////////
class Version::LevelRootAndChildNumIterator : public Iterator {
 public:
  LevelRootAndChildNumIterator(const InternalKeyComparator& icmp,
                               const uint32_t level,
                               const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        level_(level),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    // 哪一层
    EncodeFixed32(value_buf_, level_);
    // 哪一个元数据
    EncodeFixed32(value_buf_+4, index_);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;
  const uint32_t level_;

  // 内容是compaction的哪一层，哪一个元数据
  mutable char value_buf_[16];
};
///////////////////////////////////////

///////////////////////////////////////
// arg3 = 8 表示是compaction，0-7表示为level
static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value,
                                 void* arg2, 
                                 int arg3 = 7) {
///////////////////////////////////////
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

///////////////////////////////////////
// arg3 = 7 表示是compaction，0-6表示为level
static Iterator* GetMixedterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value,
                                 void* arg2, 
                                 int arg3 = 7) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  Compaction* c = reinterpret_cast<Compaction*>(arg2);
  //std::vector<BlockInfo>* chosenPosition = reinterpret_cast<std::vector<BlockInfo>*>(arg2);
  //const InternalKeyComparator icmp_;
  uint32_t which = DecodeFixed32(file_value.data());
  uint64_t index = DecodeFixed32(file_value.data()+4);
  
  //if (file_value.size() != 16) {
  //  return NewErrorIterator(
  //      Status::Corruption("FileReader invoked with unexpected value"));
  //} else {
    //std::vector<BlockInfo>* chosenPosition = &(version->current()->valid
  Iterator* it = cache->NewMixedIterator(options,
                            c->GetInputVersion()->GetVset()->Icmp(), 
                            &(c->input(which, index)->child_file),
                            (c->input(which, index)->bim_->ValidBlock()));
  return it;
  //}
}
///////////////////////////////////////

///////////////////////////////////////
// arg3 = 7 表示是compaction，0-6表示为level
static Iterator* ScanGetMixedterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value,
                                 void* arg2, 
                                 int arg3 = 7) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  //std::vector<BlockInfo>* chosenPosition = reinterpret_cast<std::vector<BlockInfo>*>(arg2);
  //const InternalKeyComparator icmp_;
  uint32_t level = DecodeFixed32(file_value.data());
  uint64_t index = DecodeFixed32(file_value.data()+4);
  Version* v = reinterpret_cast<Version*>(arg2);
  std::vector<FileMetaData*>* files = v->GetLevelFiles(level);
  
  //if (file_value.size() != 16) {
  //  return NewErrorIterator(
  //      Status::Corruption("FileReader invoked with unexpected value"));
  //} else {
    //std::vector<BlockInfo>* chosenPosition = &(version->current()->valid
  Iterator* it = cache->NewMixedIterator(options,
                            v->GetVset()->Icmp(), 
                            &((*files)[index]->child_file),
                            ((*files)[index]->bim_->ValidBlock()));
  (*files)[index]->bim_->refs++;
  scan_b_.push_back((*files)[index]->bim_);
  return it;
  //}
}
///////////////////////////////////////

///////////////////////////////////////
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options, NULL);
}
///////////////////////////////////////

///////////////////////////////////////
Iterator* Version::FGKVNewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelRootAndChildNumIterator(vset_->icmp_, level, &files_[level]),
      &ScanGetMixedterator, vset_->table_cache_, options, (void*)(this));
}
///////////////////////////////////////

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

///////////////////////////////////////
void Version::FGKVAddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewMixedIterator(options, GetVset()->icmp_, 
                &(files_[0][i]->child_file), files_[0][i]->bim_->ValidBlock()));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(FGKVNewConcatenatingIterator(options, level));
    }
  }
}
///////////////////////////////////////

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats, 
                    LevelAndStage* lands) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // // 读的时候不能删除元数据
          // f->refs++;
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;

      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) {
        files = NULL;
        num_files = 0;
      } else {
        tmp2 = files[index];
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = NULL;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      ///////////////////////////////////////
      s = vset_->table_cache_->Get(options, level, f, ikey, &saver, SaveValue, lands);
      ///////////////////////////////////////
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }

      // // 读完释放
      // f->refs--;
      // if (f->refs <= 0) { 
      //   delete f;
      // }

    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      ///////////////////////////////////////
      f->fmd_stage_[0] = 1;
      ///////////////////////////////////////
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

///////////////////////////////////////
std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}
///////////////////////////////////////

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      // 更新从表compaction产生的新的有效块索引
      if (f->bkp_bim_ != NULL) {
        BlockInfoManager* tmp = f->bim_;
        f->bim_ = f->bkp_bim_;
        f->bkp_bim_ = NULL;
        tmp->refs --;
        if (tmp->refs <= 0) delete tmp;
      }
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      //// <FMD_STAGE>
      f->fmd_stage_[0] = 0;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp, 
                       ///////////////////////////////////////
                       FG_Stats* fg_stats)
                       ///////////////////////////////////////
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL), 
      ///////////////////////////////////////
      fg_stats_(fg_stats) {
      ///////////////////////////////////////
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

///////////////////////////////////////
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu, Compaction* c) {

  //// <COMPACTION_STAGE>
  uint8_t stg;
  if (c != NULL) { 
    stg = c->stage_;
    c->stage_ = 99;
  }

  //// <FMD_STAGE>
  if (c != NULL)
  c->AddFMDStage(21, 21);

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);

    ////// <fg_stats>
    //uint64_t fg_apply_start = env_->NowMicros();
    builder.Apply(edit);
    //fg_stats.fg_apply_time = (env_->NowMicros() - fg_apply_start);

    //// <fg_stats>
    //uint64_t fg_saveto_start = env_->NowMicros();
    builder.SaveTo(v);
    //fg_stats.fg_saveto_time = (env_->NowMicros() - fg_saveto_start);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  if (c != NULL) { 
    c->stage_ = stg;
  }

  return s;
}
///////////////////////////////////////

Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == NULL);
  assert(descriptor_log_ == NULL);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == NULL);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      // score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);

      ////////////////////////////////////////
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
      ///////////////////////////////////////
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

///////////////////////////////////////
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      ///////////////////////////////////////
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest, f->bim_, f->child_file, f->fmd_stage_);
      ///////////////////////////////////////
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}
///////////////////////////////////////

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

///////////////////////////////////////
#ifndef PATCH_PLUS_COMPACTION
void VersionSet::GetChosenBlocks(Compaction* c,
                 std::vector< std::vector<BlockInfo*> >* chosenBlock,
                 Iterator* iiter) {
  //// <FG_STATS>
  uint64_t fg_gcp_start = env_->NowMicros();

  for (size_t i = 0; i < c->inputs_[1].size(); i++) {
    c->fg_delete_size_.push_back(0);
    FileMetaData* f = c->inputs_[1][i];
    BlockInfoManager* bIM = f->bim_;
    std::vector<BlockInfo*> pVec;
    // 如果簇合并
    if (c->fg_drop_[i]) {
      chosenBlock->push_back(*(bIM->ValidBlock()));
      c->fg_delete_size_[i] = f->file_size;

      // iiter略过当前sstable，去和inputs_[1]中的下一族sstable比较
      Slice fg_lastKey = f->largest.Encode();
      iiter->Seek(fg_lastKey);
      if ( !iiter->Valid() ) continue;
      if (ExtractUserKey(iiter->key()).compare(ExtractUserKey(fg_lastKey)) == 0) { 
        iiter->Next();
      }
    } else {
      uint64_t fg_block_size = 0;
      int childNumber = f->child_file.size();
      uint64_t* deletedSizeEachChild = new uint64_t[childNumber]();
      // 需要全选的补丁号
      std::vector<int> chosenChild;
      std::vector<int> chosenPosition;
      bool moved = true;
      std::vector<BlockInfo*>* vB = bIM->ValidBlock();
      int pos = 0, vBSize = vB->size();
      while (iiter->Valid()) {
        while (pos < vBSize && 
              ExtractUserKey(iiter->key()).compare(ExtractUserKey((*vB)[pos]->index_key)) > 0) {
          pos++;
          moved = true;
        }
        if (pos < vBSize && moved) {
          // pVec.push_back((*vB)[pos]);
          chosenPosition.push_back(pos);
          f->child_file[(*vB)[pos]->file].invalid_blocks_count += 1;

          Slice fg_block_handle((*vB)[pos]->block_handle);
          if (GetVarint64(&fg_block_handle, &fg_block_size) &&
                    GetVarint64(&fg_block_handle, &fg_block_size)) { 
            //c->fg_delete_size_[i] += fg_block_size;
            deletedSizeEachChild[(*vB)[pos]->file] += fg_block_size;
            f->child_file[(*vB)[pos]->file].invalid_size += fg_block_size;
          }

          moved = false;
        }
        if ( pos >= vBSize ) {
          break;
        }
        iiter->Next();
      } // <end while(iiter->Valid())>

      for (int childNumber = 0; childNumber < f->child_file.size(); childNumber ++) {
        if (f->child_file[childNumber].is_valid && 
              f->child_file[childNumber].invalid_size > f->child_file[childNumber].file_size * 0.8) {
          chosenChild.push_back(childNumber);
          f->child_file[childNumber].invalid_size = f->child_file[childNumber].file_size;
          f->child_file[childNumber].invalid_blocks_count = f->child_file[childNumber].total_blocks_count;
        }
      }
      int start = 0;
      int ccSize = chosenChild.size();
      if (!chosenPosition.empty()) {
        for (int j = 0; j < vBSize; j++) {
          // 已经被选中的
          if (j == chosenPosition[start]) {
            pVec.push_back((*vB)[j]);
            start += 1;

            // Slice fg_block_handle((*vB)[j]->block_handle);
            // if (GetVarint64(&fg_block_handle, &fg_block_size) &&
            //           GetVarint64(&fg_block_handle, &fg_block_size)) { 
            //   c->fg_delete_size_[i] += fg_block_size;
            // }
          } else {
            for (int k = 0; k < ccSize; k++) {
              // 属于垃圾回收选中的
              if ((*vB)[j]->file == chosenChild[k]) {
                pVec.push_back((*vB)[j]);
              }
            }
          }
        }
      }
      uint64_t originalDeletedSize = 0;
      for (int k = 0; k < childNumber; k++) {
        if (f->child_file[k].is_valid && f->child_file[k].invalid_size >= f->child_file[k].file_size) {
          c->fg_delete_size_[i] += f->child_file[k].file_size;
        } else {
          c->fg_delete_size_[i] += deletedSizeEachChild[k];
        }
        // 簇合并不受patch+ compaction的影响
        originalDeletedSize += deletedSizeEachChild[k];
      }
      // // 如果该Cluster没有一个命中块，那就选中它的最后一个block
      // // (这是有可能的，比如该Cluster中只有寥寥数个kv对，inputs_[0]完全有可能
      // // 没有kv对对应它的block，但是inputs_[0]整体的key range又覆盖该Cluster)
      // if (pVec.size() == 0) {
      //   pVec.push_back((*vB)[--pos]);

      //   Slice fg_block_handle((*vB)[pos]->block_handle);
      //   if (GetVarint64(&fg_block_handle, &fg_block_size) &&
      //             GetVarint64(&fg_block_handle, &fg_block_size)) { 
      //     c->fg_delete_size_[i] += fg_block_size;
      //   }

      // }
      // 如果选中的数据量占总数据量的75%以上，就标记做簇合并
      if ( originalDeletedSize > c->inputs_[1][i]->file_size * 0.80 ) { 
        chosenBlock->push_back(*(bIM->ValidBlock()));
        c->fg_delete_size_[i] = c->inputs_[1][i]->file_size;
        c->fg_drop_[i] = true;
        continue;
      }
      if (!c->fg_drop_[i]) {
        fg_stats_->chosen_child += ccSize;
        for (int cc = 0; cc < ccSize; cc++) {
          fg_stats_->chosen_child_size += c->inputs_[1][i]->child_file[cc].file_size;
        }
      }
      // // 如果没有命中块，不删除它的元数据
      // if (pVec.empty()) c->fg_drop_[i] = false;
      chosenBlock->push_back(pVec);
    } // <end if(簇合并) ... else>
  } // <end for (size_t i = 0; i < c->inputs_[1].size(); i++)>
  
  //// <FG_STATS>
  fg_stats_->fg_getchosenpositions_time += (env_->NowMicros() - fg_gcp_start);

}
#else
void VersionSet::GetChosenBlocks(Compaction* c,
                 std::vector< std::vector<BlockInfo*> >* chosenBlock,
                 Iterator* iiter) {
  //// <FG_STATS>
  uint64_t fg_gcp_start = env_->NowMicros();

  for (size_t i = 0; i < c->inputs_[1].size(); i++) {
    c->fg_delete_size_.push_back(0);
    FileMetaData* f = c->inputs_[1][i];
    BlockInfoManager* bIM = f->bim_;
    std::vector<BlockInfo*> pVec;
    // 如果簇合并
    if (c->fg_drop_[i]) {
      chosenBlock->push_back(*(bIM->ValidBlock()));
      c->fg_delete_size_[i] = f->file_size;

      // iiter略过当前sstable，去和inputs_[1]中的下一族sstable比较
      Slice fg_lastKey = f->largest.Encode();
      iiter->Seek(fg_lastKey);
      if ( !iiter->Valid() ) continue;
      if (ExtractUserKey(iiter->key()).compare(ExtractUserKey(fg_lastKey)) == 0) { 
        iiter->Next();
      }
    } else {
      uint64_t fg_block_size = 0;
      int childNumber = f->child_file.size();
      uint64_t* deletedSizeEachChild = new uint64_t[childNumber]();
      bool moved = true;
      std::vector<BlockInfo*>* vB = bIM->ValidBlock();
      int pos = 0, vBSize = vB->size();
      while (iiter->Valid()) {
        while (pos < vBSize && 
              ExtractUserKey(iiter->key()).compare(ExtractUserKey((*vB)[pos]->index_key)) > 0) {
          pos++;
          moved = true;
        }
        if (pos < vBSize && moved) {
          pVec.push_back((*vB)[pos]);
          f->child_file[(*vB)[pos]->file].invalid_blocks_count += 1;

          Slice fg_block_handle((*vB)[pos]->block_handle);
          if (GetVarint64(&fg_block_handle, &fg_block_size) &&
                    GetVarint64(&fg_block_handle, &fg_block_size)) { 
            //c->fg_delete_size_[i] += fg_block_size;
            deletedSizeEachChild[(*vB)[pos]->file] += fg_block_size;
            f->child_file[(*vB)[pos]->file].invalid_size += fg_block_size;
          }

          moved = false;
        }
        if ( pos >= vBSize ) {
          break;
        }
        iiter->Next();
      } // <end while(iiter->Valid())>

      uint64_t originalDeletedSize = 0;
      for (int k = 0; k < childNumber; k++) {
        c->fg_delete_size_[i] += deletedSizeEachChild[k];
        // 簇合并不受patch+ compaction的影响
        originalDeletedSize += deletedSizeEachChild[k];
      }
      if ( originalDeletedSize > c->inputs_[1][i]->file_size * 0.80 ) { 
        chosenBlock->push_back(*(bIM->ValidBlock()));
        c->fg_delete_size_[i] = c->inputs_[1][i]->file_size;
        c->fg_drop_[i] = true;
        continue;
      }
      // // 如果没有命中块，不删除它的元数据
      // if (pVec.empty()) c->fg_drop_[i] = false;
      chosenBlock->push_back(pVec);
    } // <end if(簇合并) ... else>
  } // <end for (size_t i = 0; i < c->inputs_[1].size(); i++)>
  
  //// <FG_STATS>
  fg_stats_->fg_getchosenpositions_time += (env_->NowMicros() - fg_gcp_start);

}
#endif
///////////////////////////////////////

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

///////////////////////////////////////
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        //live->insert(files[i]->number);
        for(int j = 0; j < files[i]->child_file.size(); j ++){
          // // 把该族的所有文件都添加进live
          // live->insert(files[i]->child_file[j].number);
          // 将未全选中的从表添加进live
          if (files[i]->child_file[j].is_valid) {
            live->insert(files[i]->child_file[j].number);
          } 
          // else if (files[i]->child_file[j].invalid_blocks_count == files[i]->child_file[j].total_blocks_count) {
          //   files[i]->child_file[j].is_valid = false;
          // }
        }
      }
    }
  }
}
///////////////////////////////////////

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewMixedIterator(options, 
                  icmp_, &(c->input(which, i)->child_file),
                  (c->input(which, i)->bim_->ValidBlock()));
        }
      } else {
        // Create concatenating iterator for the files from this level
        ///////////////////////////////////////
        list[num++] = NewTwoLevelIterator(
                new Version::LevelRootAndChildNumIterator(icmp_, which, &c->inputs_[which]),
                &GetMixedterator, table_cache_, options, c);
        ///////////////////////////////////////
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

///////////////////////////////////////
Iterator* VersionSet::LevelInputIterator(Compaction* c, int level, int which){
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  if(c->level_ == 0){
    const int space = c->inputs_[0].size();
    Iterator** list = new Iterator*[space];
    const std::vector<FileMetaData*>& files = c->inputs_[0];

    int num = 0;
    for (size_t i = 0; i < files.size(); i++) {
      list[num++] = table_cache_->NewIterator(
          options, files[i]->number, files[i]->file_size);
    }

    assert(num <= space);
    Iterator* result = NewMergingIterator(&icmp_, list, num);
    delete[] list;
    return result;
  }else{
    return NewTwoLevelIterator(
        new Version::LevelRootAndChildNumIterator(icmp_, which, &c->inputs_[which]),
        &GetMixedterator, table_cache_, options, c);
  }
}
///////////////////////////////////////

///////////////////////////////////////
// 2021_3_15大改
//Iterator* VersionSet::MyMakeInputIterator(bool isRoot, 
//                                          Iterator* iiter,
//                                          Iterator* jiter){
//
//  Iterator *result = new IteratorMerger(&icmp_, isRoot, iiter, jiter);
//  return result;
//}
///////////////////////////////////////

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        ///////////////////////////////////////
        f->fmd_stage_[0] = 1;
        ///////////////////////////////////////
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
      ///////////////////////////////////////
      current_->files_[level][0]->fmd_stage_[0] = 1;
      ///////////////////////////////////////
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
    ///////////////////////////////////////
    current_->file_to_compact_->fmd_stage_[0] = 1;
    ///////////////////////////////////////  
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  ///////////////////////////////////////
  c->SetInputs1DropFlag();
  ///////////////////////////////////////

  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange (
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    ///////////////////////////////////////
    // const uint64_t limit = MaxFileSizeForLevel(options_, level);
    const uint64_t limit = MaxClusterSizeForLevel(level);
    ///////////////////////////////////////
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

///////////////////////////////////////
Compaction::Compaction(const Options* options, int level)
    : stage_(0), 
      level_(level),
      // max_output_file_size_(MaxFileSizeForLevel(options, level)),
      ///////////////////////////////////////
      max_output_file_size_(OriginalClusterSizeForLevel(level+1)),
      ///////////////////////////////////////
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}
///////////////////////////////////////

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0/* &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_)*/);
}

///////////////////////////////////////
// AddInputDeletions标记删除的实际上是元数据，元数据中
// 直接记录了根表的文件号，所以只需要把该文件号做标记，
// 在版本更新的时候删除的是该文件号对应的元数据
void Compaction::AddInputDeletions(VersionEdit* edit) {
  // // 至此认为compaction结束了
  // ResetFMDStage();
  for (size_t i = 0; i < inputs_[0].size(); i++) {
      edit->DeleteFile(level_, inputs_[0][i]->number);
  }
  if (level_ == 0) {
    for (size_t i = 0; i < inputs_[1].size(); i++) {
      edit->DeleteFile(level_ + 1, inputs_[1][i]->number);
    }
  } else {
    for (size_t i = 0; i < inputs_[1].size(); i++) {
      if (fg_drop_[i]) {
        edit->DeleteFile(level_ + 1, inputs_[1][i]->number);
      }
    }
  }

  // for (int which = 0; which < 2; which++) {
  //   for (size_t i = 0; i < inputs_[which].size(); i++) {
  //     edit->DeleteFile(level_ + which, inputs_[which][i]->number);
  //   }
  // }
}
///////////////////////////////////////

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

///////////////////////////////////////
void Compaction::AddFMDStage(uint8_t input0_stage, uint8_t input1_stage) { 
  //if (input0_stage != 0) { 
    for (size_t i = 0; i < inputs_[0].size(); i++) {
      inputs_[0][i]->fmd_stage_.push_back(input0_stage);
    }
  //}
  //if (input1_stage != 0) { 
    for (size_t i = 0; i < inputs_[1].size(); i++) {
      inputs_[1][i]->fmd_stage_.push_back(input1_stage);
    }
  //}
}

void Compaction::ResetFMDStage() { 
  for (size_t i = 0; i < inputs_[0].size(); i++) { 
    inputs_[0][i]->fmd_stage_.resize(1);
    std::vector<uint8_t>(inputs_[0][i]->fmd_stage_).swap(inputs_[0][i]->fmd_stage_);
    inputs_[0][i]->fmd_stage_[0] = 0;
  }
  for (size_t i = 0; i < inputs_[1].size(); i++) { 
    inputs_[1][i]->fmd_stage_.resize(1);
    std::vector<uint8_t>(inputs_[1][i]->fmd_stage_).swap(inputs_[1][i]->fmd_stage_);
    inputs_[1][i]->fmd_stage_[0] = 0;
  }
}
///////////////////////////////////////

///////////////////////////////////////
// void Compaction::SetInputs1DropFlag() {
//   for (size_t i = 0; i < inputs_[1].size(); i++) {
//     if ( /*(level_ == 2 && inputs_[1][i]->child_file.size() >= 10) 
//             || inputs_[1][i]->file_size >= ((level_+1)*MaxOutputFileSize())*/
//           inputs_[1][i]->child_file.size() >= 16 
//             || inputs_[1][i]->file_size >= (10)*(inputs_[1][i]->child_file[0].file_size) ) {
//       fg_drop_.push_back(true);
//       input_version_->vset_->fg_stats_->original_cluster_compaction_count += 1;
//     } else {
//       fg_drop_.push_back(false);
//     }
//   }
// }

void Compaction::SetInputs1DropFlag() {
  for (size_t i = 0; i < inputs_[1].size(); i++) {
    if (inputs_[1][i]->child_file.size() >= 11 
            || inputs_[1][i]->file_size >= MaxClusterSizeForLevel(level_+1)) {
      fg_drop_.push_back(true);
      input_version_->vset_->fg_stats_->original_cluster_compaction_count += 1;
    } else {
      fg_drop_.push_back(false);
    }
  }
}
///////////////////////////////////////

}  // namespace leveldb
