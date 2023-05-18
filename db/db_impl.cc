// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

///////////////////////////////////////
#include "table/iterator_merger.h"
#include <fstream>
//#include "util/fg_stats.h"
///////////////////////////////////////

// ///////////////////////////////////////
// FG_Stats fg_stats;
// ///////////////////////////////////////

///////////////////////////////////////
//#define OUTPUT_FILE_SIZE
#ifdef OUTPUT_FILE_SIZE
FILE* file_size[6];
static FILE *file_size1 = fopen("/home/chen/level1.txt", "w+");
static FILE *file_size2 = fopen("/home/chen/level2.txt", "w+");
static FILE *file_size3 = fopen("/home/chen/level3.txt", "w+");
static FILE *file_size4 = fopen("/home/chen/level4.txt", "w+");
static FILE *file_size5 = fopen("/home/chen/level5.txt", "w+");
static FILE *file_size6 = fopen("/home/chen/level6.txt", "w+");
static uint64_t upper_size[7];
static uint64_t lower_size[7];
#endif
///////////////////////////////////////

namespace leveldb {

// 保存scan用到的sstable的bim_
std::vector<BlockInfoManager*> scan_b_;

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  ///////////////////////////////////////
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;

    // 该Output结构体是由inputs_[1]中的那个文件生成的
    size_t which_file;
    // 是否是生成cluster，在InstallCompactionResults中读取它的状态
    bool root;
    std::vector<ChildMetaData> child_file;
    mutable BlockInfoManager* bi_m;
    std::vector<uint8_t> fmd_stage;

    Output() : root(false), bi_m(new BlockInfoManager()) {
      fmd_stage.push_back(0);
      child_file.clear();
      bi_m->refs = 1;
    }
    // 对于Output，是否可以把bim的内容全复制
    Output(const Output& outp) : number(outp.number), file_size(outp.file_size), 
            smallest(outp.smallest), largest(outp.largest), which_file(outp.which_file), 
            root(outp.root), bi_m(outp.bi_m) { 
      //bi_m->refs++;
      outp.bi_m = NULL;
      child_file.assign(outp.child_file.begin(), outp.child_file.end());
      fmd_stage.assign(outp.fmd_stage.begin(), outp.fmd_stage.end());
      //bi_m = new BlockInfoManager(outp.bi_m);
    }
    ~Output() { 
      std::vector<ChildMetaData>().swap(child_file);
      if (bi_m != NULL) { 
        bi_m->refs--;
        if (bi_m->refs <= 0) {
          delete bi_m;
          bi_m = NULL;
        }
      } 
    }

  };
  ///////////////////////////////////////
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL), 
      ///////////////////////////////////////
      fg_stats_(new FG_Stats()), 
      lands_(new LevelAndStage()) {
      ///////////////////////////////////////
  has_imm_.Release_Store(NULL);

  ///////////////////////////////////////
  pout = fopen("/home/chen/pout.txt","w+");
  ///////////////////////////////////////

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;

  ///////////////////////////////////////
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size, fg_stats_);
  ///////////////////////////////////////

  ///////////////////////////////////////
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_, fg_stats_);
  ///////////////////////////////////////
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

// void DBImpl::DeleteObsoleteFiles() {
//   if (!bg_error_.ok()) {
//     // After a background error, we don't know whether a new version may
//     // or may not have been committed, so we cannot safely garbage collect.
//     return;
//   }

//   // Make a set of all of the live files
//   std::set<uint64_t> live = pending_outputs_;
//   versions_->AddLiveFiles(&live);

//   ///////////////////////////////////////
//   // 11个目录，10个子文件夹
//   std::vector<std::string> filenames[11];
//   // 处理非.ldb文件
//   env_->GetChildren(dbname_, &filenames[10]); // Ignoring errors on purpose
//   for (char c = 48; c < 58; c++) { 
//     std::string dbpath = std::string("/home/chen/ssd/") + c;
//     env_->GetChildren(dbpath, &filenames[c-48]);
//   } 
//   ///////////////////////////////////////
  
//   uint64_t number;
//   FileType type;
  
//   for (char dir = 48; dir < 58; dir++) { 
//     for (size_t i = 0; i < filenames[dir-48].size(); i++) {
//       if (ParseFileName(filenames[dir-48][i], &number, &type)) {
//         bool keep = true;
//         assert(type == kTableFile);
//         keep = (live.find(number) != live.end());

//         if (!keep) {
//           table_cache_->Evict(number);
//           Log(options_.info_log, "Delete type=%d #%lld\n",
//               int(type),
//               static_cast<unsigned long long>(number));
//           env_->DeleteFile(std::string("/home/chen/ssd/") + dir + '/' + filenames[dir-48][i]);
//         }
//       }
//     }
//   }

//   // 处理非.ldb文件
//   for (size_t i = 0; i < filenames[10].size(); i++) {
//     if (ParseFileName(filenames[10][i], &number, &type)) {
//       bool keep = true;
//       switch (type) {
//         case kLogFile:
//           keep = ((number >= versions_->LogNumber()) ||
//                   (number == versions_->PrevLogNumber()));
//           break;
//         case kDescriptorFile:
//           // Keep my manifest file, and any newer incarnations'
//           // (in case there is a race that allows other incarnations)
//           keep = (number >= versions_->ManifestFileNumber());
//           break;
//         case kTempFile:
//           // Any temp files that are currently being written to must
//           // be recorded in pending_outputs_, which is inserted into "live"
//           keep = (live.find(number) != live.end());
//           break;
//         case kCurrentFile:
//         case kDBLockFile:
//         case kInfoLogFile:
//           keep = true;
//           break;
//       }

//       if (!keep) {
//         Log(options_.info_log, "Delete type=%d #%lld\n",
//             int(type),
//             static_cast<unsigned long long>(number));
//         env_->DeleteFile(dbname_ + "/" + filenames[10][i]);
//       }
//     }
//   }
// }

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

///////////////////////////////////////
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  meta.bim_ = new BlockInfoManager();
  meta.bim_->refs = 1;
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  ///////////////////////////////////////
  // // 记录这个文件的block信息
  // meta.bim_ = new BlockInfoManager();
  ///////////////////////////////////////
  Status s;
  {
    mutex_.Unlock();
    ///////////////////////////////////////
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta, fg_stats_);
    // 元数据中child_file的第一个元素是根表
    ChildMetaData child;
    child.number = meta.number;
    child.file_size = meta.file_size;
    child.total_blocks_count = meta.bim_->ValidBlock()->size();
    meta.child_file.push_back(child);
    ///////////////////////////////////////
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    // const Slice min_user_key = meta.smallest.user_key();
    // const Slice max_user_key = meta.largest.user_key();
    // if (base != NULL) {
    //   level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    // }
    ///////////////////////////////////////
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest, meta.bim_, meta.child_file, meta.fmd_stage_);
    ///////////////////////////////////////
    // // 用完要释放
    // delete meta.bim_;
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}
///////////////////////////////////////

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

///////////////////////////////////////
void DBImpl::PrintMyStats() { 
  fprintf(stderr, "总写入数据量:\t %lld \n", static_cast<unsigned long long>(fg_stats_->fg_total_write));

  Slice token = "leveldb.stats";
  std::string stats;
  GetProperty(token, &stats);
  fprintf(stderr, "\n%s\n", stats.c_str());
  
  fprintf(stderr, "GetChosenBlocks时间:\t %f \n", (fg_stats_->fg_getchosenpositions_time * 1e-6));

  for (int i = 0; i < 6; i ++) { 
    fprintf(stderr, "PickCompaction[%d]时间:\t %f \n", i, (fg_stats_->pick_compaction_time[i] * 1e-6));
  }
  fprintf(stderr, "\n");

  fprintf(stderr, "L0Compaction次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->l0_doc_count));
  fprintf(stderr, "L0Compaction时间:\t %f \n\n", (fg_stats_->l0_doc_time * 1e-6));

  fprintf(stderr, "Cluster Compaction次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->fg_root_compaction_count));
  fprintf(stderr, "Cluster Compaction时间:\t %f \n\n", ((fg_stats_->fg_root_compaction_time -
                                      fg_stats_->minor_compaction_time_in_root) * 1e-6));

  fprintf(stderr, "Patch Compaction次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->fg_child_compaction_count));
  fprintf(stderr, "Patch Compaction时间:\t %f \n\n", ((fg_stats_->fg_child_compaction_time - 
                                      fg_stats_->minor_compaction_time_in_child) * 1e-6));

  fprintf(stderr, "Memtable Get次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->mem_get_count));
  fprintf(stderr, "Memtable Get时间:\t %f \n", (fg_stats_->mem_get_time * 1e-6));
  fprintf(stderr, "TableCache Get次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->table_cache_get_count));
  fprintf(stderr, "TableCache Get时间:\t %f \n\n", (fg_stats_->table_cache_get_time * 1e-6));

  fprintf(stderr, "查找Index时间:\t %f \n", (fg_stats_->find_index_key_time * 1e-6));
  fprintf(stderr, "FindTable时间:\t %f \n", (fg_stats_->find_table_time * 1e-6));
  fprintf(stderr, "InternalGet次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->internal_get_count));
  fprintf(stderr, "InternalGet时间:\t %f \n", (fg_stats_->internal_get_time * 1e-6));
  fprintf(stderr, "InternalGet中BlockReader时间:\t %f \n", (fg_stats_->internal_get_seek_data_block_time * 1e-6));
  fprintf(stderr, "读中调用BlockReader次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_count));
  fprintf(stderr, "读中调用BlockReader时间:\t %f \n", (fg_stats_->block_reader_r_time * 1e-6));
  fprintf(stderr, "读中BlockReader中访问缓存次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_read_block_cache_count));
  fprintf(stderr, "读中BlockReader中访问磁盘Block次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_read_block_count));
  fprintf(stderr, "读中BlockReader中访问磁盘Block时间:\t %f \n", (fg_stats_->block_reader_r_read_block_time * 1e-6));
  

  fprintf(stderr, "写中调用BlockReader次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_w_count));
  fprintf(stderr, "写中调用BlockReader时间:\t %f \n", (fg_stats_->block_reader_w_time * 1e-6));
  fprintf(stderr, "写中BlockReader中访问磁盘Block时间:\t %f \n", (fg_stats_->block_reader_w_read_block_time * 1e-6));

  fprintf(stderr, "WriteBlock次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->write_block_count));
  fprintf(stderr, "WriteBlock时间:\t %f \n\n", (fg_stats_->write_block_time * 1e-6));

  for (int i = 0; i < 7; i ++){ 
    fprintf(stderr, "L%d_ReadBlock中file->Read()次数:\t %lld \n", i, static_cast<unsigned long long>(Table::read_block_stats_[i][0]));
    fprintf(stderr, "L%d_ReadBlock中file->Read()时间:\t %f \n", i, (Table::read_block_stats_[i][1] * 1e-6));
    fprintf(stderr, "L%d_ReadBlock中file->Read()大小:\t %lld \n\n", i, static_cast<unsigned long long>(Table::read_block_stats_[i][2]));
  }
  fprintf(stderr, "Compaction中ReadBlock中file->Read()次数:\t %lld \n", static_cast<unsigned long long>(Table::read_block_stats_[7][0]));
  fprintf(stderr, "Compaction中ReadBlock中file->Read()时间:\t %f \n", (Table::read_block_stats_[7][1] * 1e-6));
  fprintf(stderr, "Compaction中ReadBlock中file->Read()大小:\t %lld \n\n", static_cast<unsigned long long>(Table::read_block_stats_[7][2]));

  for (int i = 0; i < 7; i ++){ 
    fprintf(stderr, "L%d_Compaction中写Block个数: %lld \n", i, static_cast<unsigned long long>(fg_stats_->add_block_count[i]));
  }
  fprintf(stderr, "全选的补丁个数:\t %d\n", fg_stats_->chosen_child);
  fprintf(stderr, "全选的补丁大小:\t %lld \n", static_cast<unsigned long long>(fg_stats_->chosen_child_size));

  fprintf(stderr, "簇合并平均补丁数:\t %f \n", (float)fg_stats_->total_child_number_in_cluster_compaction / fg_stats_->fg_root_compaction_count);
  fprintf(stderr, "原生簇合并数:\t %d \n", fg_stats_->original_cluster_compaction_count);
  system("ps aux | grep db_bench");
  system("ps aux | grep ycsbc");

#ifdef OUTPUT_FILE_SIZE
  for (int i = 0; i < 7; i ++){ 
    fprintf(stderr, "level %d : %lld----%lld \n", i, static_cast<unsigned long long>(upper_size[i]), static_cast<unsigned long long>(lower_size[i]));
  }
#endif
  fflush(stderr);

  fg_stats_->Reset();
  for (int i = 0; i < 8; i ++) { 
    Table::read_block_stats_[i][0] = 0;
    Table::read_block_stats_[i][1] = 0;
    Table::read_block_stats_[i][2] = 0;
  }
}
///////////////////////////////////////

///////////////////////////////////////
void DBImpl::ReleaseScanb() { 
  for (int i = 0; i < scan_b_.size(); i++) {
    scan_b_[i]->refs--;
    if (scan_b_[i]->refs <= 0) {
      delete scan_b_[i];
    }
  }
  scan_b_.clear();
}
///////////////////////////////////////

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

///////////////////////////////////////
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {

    //// <FG_STATS>
    uint64_t pc_start = env_->NowMicros();
    c = versions_->PickCompaction();
    fg_stats_->pick_compaction_time[c->level()] += env_->NowMicros() - pc_start;

  }

  if (c != NULL) { 
    lands_->level = c->level();
    lands_->stage = &(c->stage_);
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {

  //// <COMPACTION_STAGE>
  c->stage_ = 2;

//// <FMD_STAGE>
c->AddFMDStage(2, 2);

    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    ///////////////////////////////////////
    // 直接挪到下一层的话，索引信息不用变，还用原来的
    // valid_block也要下移一层
    int level = c->level();
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest, 
                        f->largest, f->bim_, f->child_file, f->fmd_stage_);
    ///////////////////////////////////////
    status = versions_->LogAndApply(c->edit(), &mutex_, c);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    
    //// <COMPACTION_STAGE>
    c->stage_ = 3;

    //// <FMD_STAGE>
    c->AddFMDStage(3, 3);

    CompactionState* compact = new CompactionState(c);

    ///////////////////////////////////////
    //    2021_1_25
    if(compact->compaction->level() == 0){

      //// <COMPACTION_STAGE>
      c->stage_ = 4;
      // 编号11-20给DoCompactionWork

      status = DoCompactionWork(compact);
    }else{

      //// <COMPACTION_STAGE>
      c->stage_ = 5;
      // 编号31-50给DoCompactionWork

      status = DoFineGrainedCompactionWork(compact);
    }
    ///////////////////////////////////////

    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);

    //// <COMPACTION_STAGE>
    c->stage_ = 6;

    //// <FMD_STAGE>
    c->ResetFMDStage();

    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }

  //// <COMPACTION_STAGE>
  c->stage_ = 7;

  delete c;

  lands_->level = -1;
  lands_->stage = NULL;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }

}
///////////////////////////////////////

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

///////////////////////////////////////
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact,
                                bool isRoot, size_t order) {

  //// <COMPACTION_STAGE>
  uint8_t stg = compact->compaction->stage_;
  if (compact->compaction->stage_ < 30) { 
    compact->compaction->stage_ = 11;
  } else { 
    compact->compaction->stage_ = 31;
  }
  
  // //// <FMD_STAGE> <21, 21>
  // // ROOT 跳出for(file < fileNum)阶段
  // compact->compaction->SetFMDStage(21, 21);

  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    CompactionState::Output out;
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    //CompactionState::Output out;
    out.which_file = order;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    if (isRoot) { 
      out.root = true;
    }

    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file

  //std::string dbpath = std::string("/home/chen/ssd/") + file_dir[file_number % 10];

  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {

      ///////////////////////////////////////
      if(isRoot) {// 生成Cluster，把空的bim_传进入，插入
          compact->builder = new TableBuilder(options_, compact->compaction->level()+1, compact->outfile, NULL, compact->current_output()->bi_m, NULL, fg_stats_);
      } else {// 生成patch
          compact->builder = new TableBuilder(options_, compact->compaction->level()+1, compact->outfile,
                    compact->compaction->input(1, order)->bim_, compact->current_output()->bi_m, 
                    &(compact->compaction->input(1, order)->child_file), fg_stats_);
      }
      ///////////////////////////////////////

  }

  compact->compaction->stage_ = stg;

  return s;
}
///////////////////////////////////////

///////////////////////////////////////
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                Iterator* iter, bool isRoot, bool isUsingRoot, size_t order) {
  
  //// <COMPACTION_STAGE>
  uint8_t stg = compact->compaction->stage_;
  if (compact->compaction->stage_ < 30) { 
    compact->compaction->stage_ = 12;
  } else { 
    compact->compaction->stage_ = 32;
  }
  
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = iter->status();
  const uint64_t current_entries = compact->builder->NumEntries();

  uint64_t current_bytes = 0;

  if (s.ok()) {
    ///////////////////////////////////////
    if (isRoot) {

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 71;

      s = compact->builder->RootFinish(0);
      current_bytes = compact->builder->FileSize();
      ChildMetaData child;
      child.number = compact->current_output()->number;
      child.file_size = current_bytes;
      child.total_blocks_count = compact->builder->flush_blocks_count;
      compact->current_output()->child_file.push_back(child);
    } else {

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 72;

      if (isUsingRoot) {
        s = compact->builder->RootFinish(compact->compaction->input(1, order)->child_file.size());
      } else {
        s = compact->builder->Finish(compact->compaction->input(1, order)->child_file.size());
      }

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 73;

      current_bytes = compact->builder->FileSize();
      ChildMetaData child;
      child.number = compact->current_output()->number;
      child.file_size = current_bytes;
      child.total_blocks_count = compact->builder->flush_blocks_count;
      compact->compaction->input(1, order)->child_file.push_back(child);
      compact->compaction->input(1, order)->file_size = 
        compact->compaction->input(1, order)->file_size + current_bytes - compact->compaction->fg_delete_size_[order];
      // 文件的有效数据量改变了，所以allowed_seeks也要改变
      compact->compaction->input(1, order)->allowed_seeks = 
        compact->compaction->input(1, order)->allowed_seeks + (current_bytes / 16384) - 
        compact->compaction->fg_delete_size_[order] / 16384;
    }
    ///////////////////////////////////////

  } else {
    compact->builder->Abandon();
  }

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 74;

  // 修改file_size
  //const uint64_t current_bytes = compact->builder->FileSize();
  //compact->current_output()->file_size = current_bytes;
  current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;

  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 75;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 76;

  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 77;

  if (s.ok() && current_entries > 0) {
    // // Verify that the table is usable
    // Iterator* iter = table_cache_->NewIterator(ReadOptions(),
    //                                            output_number,
    //                                            current_bytes);
    // s = iter->status();
    // delete iter;
    // if (s.ok()) {
    
    // // Verify that the table is usable
    // Iterator* iter = table_cache_->NewMixedIterator(
    //                                       ReadOptions(),
    //                                       internal_comparator_, 
    //                                       &(compact->compaction->input(1, order)->child_file), 
    //                                       compact->current_output()->bi_m->ValidBlock());
    // s = iter->status();
    // delete iter;
    // if (s.ok()) {
      if(isRoot){
          Log(options_.info_log,
          "Generated root_table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
      }
      else{
          Log(options_.info_log,
          "Generated child_table[%lu] #%llu@%d: %lld keys, %lld bytes",
          compact->compaction->input(1, order)->child_file.size() - 1,
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
      }
    // }
  }

#ifdef OUTPUT_FILE_SIZE
  int level = compact->compaction->level();
  FILE *file_size;
  switch (level)
  {
  case 0:
    file_size =  file_size1;
    break;
  case 1:
    file_size =  file_size2;
    break;
  case 2:
    file_size =  file_size3;
    break;
  case 3:
    file_size =  file_size4;
    break;
  case 4:
    file_size =  file_size5;
    break;
  case 5:
    file_size =  file_size6;
    break;
  
  default:
    file_size = NULL;
    break;
  }
  int size = current_bytes / 1024 / 1024;
  if (isRoot) {
    fprintf(file_size, "root  %d\n", size);
  } else {
    fprintf(file_size, "child %d\n", size);
  }
#endif
  compact->compaction->stage_ = stg;

  return s;
}
///////////////////////////////////////


///////////////////////////////////////
Status DBImpl::InstallCompactionResults(CompactionState* compact) {

  //// <COMPACTION_STAGE>
  uint8_t stg = compact->compaction->stage_;
  if (compact->compaction->stage_ < 30) { 
    compact->compaction->stage_ = 13;
  } else { 
    compact->compaction->stage_ = 33;
  }

  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level() + 1;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    ///////////////////////////////////////
    // 只有生成的根表才需要AddFile；
    // 如果不是根表，则可能smallest和largest发生了变化，
    // 更新元数据中的smallest和largest
    if (out.root) {
      // 生成了根表，需要把它的index信息放到versions_.valid_block[level]中
      compact->compaction->edit()->AddFile(
        level, out.number, out.file_size, out.smallest,
        out.largest, out.bi_m, out.child_file, out.fmd_stage);
    } else {
      size_t file = out.which_file;
      FileMetaData* f = compact->compaction->input(1, file);
      for (int j = 0; j < f->child_file.size(); j++) {
        // if (f->child_file[j].invalid_blocks_count == f->child_file[j].total_blocks_count) {
        //   f->child_file[j].is_valid = false;
        // }
        if (f->child_file[j].invalid_size >= f->child_file[j].file_size) {
          f->child_file[j].is_valid = false;
        }
      }
      // 之后在LogAndApply中再更新bim_和bkp_bim_
      f->bkp_bim_ = out.bi_m;
      out.bi_m = NULL;
      Slice smt = out.smallest.Encode();
      Slice lrt = out.largest.Encode();
      //f->smallest.Clear();
      const InternalKeyComparator fg_icmp = versions_->Icmp();
      if( fg_icmp.Compare(smt, f->smallest.Encode()) < 0 )
        f->smallest.DecodeFrom(smt);
      // 当level层的key范围超过level+1层的时候，最后一个sstable的最大key会变
      if( fg_icmp.Compare(lrt, f->largest.Encode()) > 0 )
        f->largest.DecodeFrom(lrt);
    }
    ///////////////////////////////////////
  }

  compact->compaction->stage_ = stg;

  return versions_->LogAndApply(compact->compaction->edit(), &mutex_, compact->compaction);
}
///////////////////////////////////////

///////////////////////////////////////
Status DBImpl::DoCompactionWork(CompactionState* compact) {

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 14;

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 15;

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 16;

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary

        //// <FG_STATS>
        fg_stats_->minor_compaction_count_in_l0 += 1;

      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);

    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      ///////////////////////////////////////
      status = FinishCompactionOutputFile(compact, input, true, false, 0);
      ///////////////////////////////////////
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
    #if 0
        Log(options_.info_log,
            "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
            "%d smallest_snapshot: %d",
            ikey.user_key.ToString().c_str(),
            (int)ikey.sequence, ikey.type, kTypeValue, drop,
            compact->compaction->IsBaseLevelForKey(ikey.user_key),
            (int)last_sequence_for_key, (int)compact->smallest_snapshot);
    #endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        ///////////////////////////////////////
        status = OpenCompactionOutputFile(compact, true, 0);
        ///////////////////////////////////////
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 17;

      ///////////////////////////////////////
      compact->builder->RootAdd(key, input->value(), 0);
      ///////////////////////////////////////

      // Close output file if it is big enough
      if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
        ///////////////////////////////////////
        status = FinishCompactionOutputFile(compact, input, true, false, 0);
        ///////////////////////////////////////
        if (!status.ok()) {
          break;
        }
      }
    }

    //// <COMPACTION_STAGE>
    compact->compaction->stage_ = 18;

    input->Next();
  }

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 19;

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    ///////////////////////////////////////
    status = FinishCompactionOutputFile(compact, input, true, false, 0);
    ///////////////////////////////////////
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;

  //// <FG_STATS>
  fg_stats_->minor_compaction_time_in_l0 += imm_micros;
  fg_stats_->l0_doc_count += 1;
  fg_stats_->l0_doc_time += stats.micros;

  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}
///////////////////////////////////////

///////////////////////////////////////
Status DBImpl::DoFineGrainedCompactionWork(CompactionState* compact) {

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 34;

#ifdef OUTPUT_FILE_SIZE
  int level1 = compact->compaction->level();
  for (int i = 0; i < compact->compaction->num_input_files(0); ++i) {
    upper_size[level1] += compact->compaction->input(0, i)->file_size;
  }
  for (int i = 0; i < compact->compaction->num_input_files(1); ++i) {
    lower_size[level1] += compact->compaction->input(1, i)->file_size;
  }
#endif
  //// <FMD_STAGE> <4, 4>
  // 刚进DoFineGrainedCompactionWork
  compact->compaction->AddFMDStage(4, 4);

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 35;

  //// <FMD_STAGE> <5, 5>
  compact->compaction->AddFMDStage(5, 5);

  Iterator* iiter = versions_->LevelInputIterator(compact->compaction, 
                        compact->compaction->level(), 0);
  iiter->SeekToFirst();
  int fileNum = compact->compaction->num_input_files(1);
  std::vector< std::vector<BlockInfo*> > chosenBlock;

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 36;

  //// <FMD_STAGE> <6, 6>
  // 进入GetChosenBlocks
  compact->compaction->AddFMDStage(6, 6);

  versions_->GetChosenBlocks(compact->compaction, &chosenBlock, iiter);

  iiter->SeekToFirst();
  int file = 0;
  Status status;

  for (; file < fileNum && !shutting_down_.Acquire_Load(); file++) {
    if (chosenBlock[file].empty()) continue;

    //// <COMPACTION_STAGE>
    compact->compaction->stage_ = 37;

    //// <FMD_STAGE> <7, 7>
    // 对level+1创建迭代器
    compact->compaction->AddFMDStage(7, 7);

    ReadOptions options;
    options.verify_checksums = options_.paranoid_checks;
    options.fill_cache = false;
    Iterator* jiter = table_cache_->NewMixedIterator(options, versions_->Icmp(),
              &(compact->compaction->input(1, file)->child_file), 
              &chosenBlock[file]);
    // 如果isRoot为false的话，IteratorMerger的有效性和传进去的第二个迭代器一致
    // 否则只要传进去的两个迭代器有一个有效，input即有效
    Iterator* input = NewIteratorMerger(&(versions_->Icmp()), iiter, jiter);

    input->SeekToFirst();

    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    
    bool isRoot = compact->compaction->fg_drop_[file];
    if (isRoot) {

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 38;

      // fprintf(pout, "从表数: %d\t簇大小: %lld\n", compact->compaction->input(1, file)->child_file.size(), 
      //     static_cast<unsigned long long>(compact->compaction->input(1, file)->file_size));
      // for (int cf = 0; cf < compact->compaction->input(1, file)->child_file.size(); cf ++) {
      //   fprintf(pout, "%lld\t", 
      //     static_cast<unsigned long long>(compact->compaction->input(1, file)->child_file[cf].file_size));
      // }
      // fprintf(pout, "\n");
      fg_stats_->total_child_number_in_cluster_compaction += compact->compaction->input(1, file)->child_file.size();

      //// <FMD_STAGE> <8, 8>
      // ROOT 进入单个FileMetaData遍历阶段
      compact->compaction->AddFMDStage(8, 8);

      //// <FG_STATS>
      uint64_t fg_root_compaction_start = env_->NowMicros();

      for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {

        // Prioritize immutable compaction work
        if (has_imm_.NoBarrier_Load() != NULL) {
          const uint64_t imm_start = env_->NowMicros();
          mutex_.Lock();
          if (imm_ != NULL) {
            CompactMemTable();
            bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary

            //// <FG_STATS>
            fg_stats_->minor_compaction_count_in_root += 1;

          }
          mutex_.Unlock();

          imm_micros += (env_->NowMicros() - imm_start);

          //// <FG_STATS>
          fg_stats_->minor_compaction_time_in_root += (env_->NowMicros() - imm_start);

        }
  
        Slice key = input->key();
        if (compact->compaction->ShouldStopBefore(key) &&//结束写当前sstable
            compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
          if (!status.ok()) {
            break;
          }
        }

        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {//错误的key
          // Do not hide error keys
          current_user_key.clear();
          has_current_user_key = false;
          last_sequence_for_key = kMaxSequenceNumber;
        } else {
          if (!has_current_user_key ||
              user_comparator()->Compare(ikey.user_key,
                                        Slice(current_user_key)) != 0) {
            current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
            has_current_user_key = true;
            last_sequence_for_key = kMaxSequenceNumber;
          }

          if (last_sequence_for_key <= compact->smallest_snapshot) {
            // Hidden by an newer entry for same user key
            drop = true;    // (A)
          }
          else if (ikey.type == kTypeDeletion &&
                    ikey.sequence <= compact->smallest_snapshot &&
                    compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
            drop = true;
          }

          last_sequence_for_key = ikey.sequence;
        }

        #if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
        #endif

        if (!drop) {
          // Open output file if necessary
          if (compact->builder == NULL) {
            status = OpenCompactionOutputFile(compact, isRoot, file);
            if (!status.ok()) {
              break;
            }
          }

          //// <COMPACTION_STAGE>
          // 39标识DecodeFrom
          compact->compaction->stage_ = 39;

          if (compact->builder->NumEntries() == 0) {
            compact->current_output()->smallest.DecodeFrom(key);
          }
          compact->current_output()->largest.DecodeFrom(key);

          //// <COMPACTION_STAGE>
          // 40标识root compaction中的builder->RootAdd
          compact->compaction->stage_ = 40;

          compact->builder->RootAdd(key, input->value(), 0);

          // // Close output file if it is big enough
          // if (compact->builder->FileSize() >=
          //             /*(4 << (20 + compact->compaction->level() - 1))*/
          //             ((compact->compaction->level() + 1) * (compact->compaction->MaxOutputFileSize()))) {
          //   status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
          //   if (!status.ok()) {
          //     break;
          //   }
          // }
          if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
            status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
            if (!status.ok()) {
              break;
            }
          }
        }    // end< if (!drop) >

        //// <COMPACTION_STAGE>
        // 40标识root compaction中的builder->Add
        compact->compaction->stage_ = 41;
        
        input->Next();
      }    // end< for (; input->Valid() && !shutting_down_.Acquire_Load(); ) >

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 42;

      //// <FMD_STAGE> <9, 9>
      // ROOT 跳出for(input->Valid())阶段
      compact->compaction->AddFMDStage(9, 9);

      if ( (file == fileNum-1) && iiter->Valid() && !shutting_down_.Acquire_Load() ) { 
        Slice key = iiter->key();
        if (compact->builder == NULL) {
          status = OpenCompactionOutputFile(compact, isRoot, file);
          if (!status.ok()) {
            break;
          }
          compact->current_output()->smallest.DecodeFrom(key);
        }
        // level层的key范围超过了level+1层，
        // 超出的部分加到当前output中，block大小按规定
        while (iiter->Valid()) {
          key = iiter->key();
          compact->current_output()->largest.DecodeFrom(key);
          compact->builder->RootAdd(key, iiter->value(), 0);
          iiter->Next();
        }
        if (status.ok() && shutting_down_.Acquire_Load()) {
          status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
        }
      } else if ( (file == fileNum-1) && !iiter->Valid() && !shutting_down_.Acquire_Load() ) {
        // level层的key范围小于level+1层，并且到了最后一个sstable，正常处理
        if (status.ok() && shutting_down_.Acquire_Load()) {
          status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
        }
      } else {
        // level+1层的处于中间的sstable，正常处理，之后处理下一个sstable
        assert(iiter->Valid());
        if (status.ok() && shutting_down_.Acquire_Load()) {
          status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
        }
      }

      if (status.ok()) {
        status = input->status();
      }
      delete jiter;
      delete input;
      input = NULL;

      //// <FG_STATS>
      fg_stats_->fg_root_compaction_count += 1;
      fg_stats_->fg_root_compaction_time += 
              (env_->NowMicros() - fg_root_compaction_start);

    } else {

      //// <FMD_STAGE> <10, 10>
      // CHILD 进入单个FileMetaData遍历阶段
      compact->compaction->AddFMDStage(10, 10);

      //// <FG_STATS>
      uint64_t fg_child_compaction_start = env_->NowMicros();

      int child = compact->compaction->input(1, file)->child_file.size();
      // patch compaction中迭代器input的有效性和level+1层的迭代器一致
      for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {

        //// <COMPACTION_STAGE>
        compact->compaction->stage_ = 43;

        // Prioritize immutable compaction work
        if (has_imm_.NoBarrier_Load() != NULL) {

          //// <COMPACTION_STAGE>
          // 61标识child compaction中的CompactMemTable
          compact->compaction->stage_ = 44;

          const uint64_t imm_start = env_->NowMicros();
          mutex_.Lock();
          if (imm_ != NULL) {
            CompactMemTable();
            bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary

            //// <FG_STATS>
            fg_stats_->minor_compaction_count_in_child += 1;

          }
          mutex_.Unlock();
          imm_micros += (env_->NowMicros() - imm_start);

          //// <FG_STATS>
          fg_stats_->minor_compaction_time_in_child += (env_->NowMicros() - imm_start);

        }

        //// <COMPACTION_STAGE>
        // 45
        compact->compaction->stage_ = 45;

        Slice key = input->key();

        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {//错误的key
          // Do not hide error keys
          current_user_key.clear();
          has_current_user_key = false;
          last_sequence_for_key = kMaxSequenceNumber;
        } else {
          if (!has_current_user_key ||
              user_comparator()->Compare(ikey.user_key,
                                        Slice(current_user_key)) != 0) {
            current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
            has_current_user_key = true;
            last_sequence_for_key = kMaxSequenceNumber;
          }

          if (last_sequence_for_key <= compact->smallest_snapshot) {
            // Hidden by an newer entry for same user key
            drop = true;    // (A)
          }
          else if (ikey.type == kTypeDeletion &&
                    ikey.sequence <= compact->smallest_snapshot &&
                    compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
            drop = true;
          }

          last_sequence_for_key = ikey.sequence;
        }

        #if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
        #endif

        if (!drop) {
          // Open output file if necessary
          if (compact->builder == NULL) {
          //生成compaction的结果文件
            status = OpenCompactionOutputFile(compact, isRoot, file);
            if (!status.ok()) {
              break;
            }
          }

          //// <COMPACTION_STAGE>
          // 46标识child compaction中的DecodeFrom
          compact->compaction->stage_ = 46;

          if (compact->builder->NumEntries() == 0) {
            compact->current_output()->smallest.DecodeFrom(key);
          }
          compact->current_output()->largest.DecodeFrom(key);

          //// <COMPACTION_STAGE>
          // 47标识child compaction中的builder->Add
          compact->compaction->stage_ = 47;

          compact->builder->Add(key, input->value(), child);
        }

        //// <COMPACTION_STAGE>
        // 48标识child compaction中的builder->Add
        compact->compaction->stage_ = 48;
        
        input->Next();
      } // end< for (; input->Valid() && !shutting_down_.Acquire_Load(); ) >

      //// <FMD_STAGE> <11, 11>
      // CHILD 跳出for(input->Valid())阶段
      compact->compaction->AddFMDStage(11, 11);

      if ( (file == fileNum-1) && iiter->Valid() && !shutting_down_.Acquire_Load() ) {

        //// <COMPACTION_STAGE>
        compact->compaction->stage_ = 49;

        // 扫尾，把jiter最后一个block的索引补上
        compact->builder->UpdateIndexEntry(child);
        // level层的key范围超过了level+1层，
        // 超出的部分加到当前output中，block大小按规定
        while (iiter->Valid()) {
          Slice key = iiter->key();
          compact->current_output()->largest.DecodeFrom(key);
          compact->builder->RootAdd(key, iiter->value(), child);
          iiter->Next();
        }
        if (status.ok() && shutting_down_.Acquire_Load()) {
          status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, true, file);
        }

      } else if ( (file == fileNum-1) && !iiter->Valid() && !shutting_down_.Acquire_Load() ) {

        //// <COMPACTION_STAGE>
        compact->compaction->stage_ = 50;

        // level层的key范围小于level+1层，并且到了最后一个sstable，正常处理
        if (status.ok() && shutting_down_.Acquire_Load()) {
            status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
            status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
        }
      } else {

        //// <COMPACTION_STAGE>
        compact->compaction->stage_ = 51;

        // level+1层的处于中间的sstable，正常处理，之后处理下一个sstable
        assert(iiter->Valid());
        if (status.ok() && shutting_down_.Acquire_Load()) {
          status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
          status = FinishCompactionOutputFile(compact, input, isRoot, false, file);
        }
      }

      //// <COMPACTION_STAGE>
      compact->compaction->stage_ = 52;
      
      if (status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during compaction");
      }
      if (status.ok()) {
        status = input->status();
      }
      delete jiter;
      delete input;
      input = NULL;

      //// <FG_STATS>
      fg_stats_->fg_child_compaction_count += 1;
      fg_stats_->fg_child_compaction_time += 
              (env_->NowMicros() - fg_child_compaction_start);

    }    // end< if(root)...else >
  }    // end< for(; file < fileNum; file++) >

  //// <COMPACTION_STAGE>
  compact->compaction->stage_ = 53;

  //// <FMD_STAGE> <12, 12>
  // ROOT 跳出for(file < fileNum)阶段
  compact->compaction->AddFMDStage(12, 12);

  delete iiter;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    stats.bytes_read += compact->compaction->input(0, i)->file_size;
  }
  for (int i = 0; i < compact->compaction->num_input_files(1); i++) {
    stats.bytes_read += compact->compaction->fg_delete_size_[i];
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  
// //// <FMD_STAGE> <0, 0>
// // ROOT 跳出for(file < fileNum)阶段
// compact->compaction->SetFMDStage(0, 0);

  return status;
}
///////////////////////////////////////

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }

  ///////////////////////////////////////
  // versions_->current()->AddIterators(options, &list);
  versions_->current()->FGKVAddIterators(options, &list);
  ///////////////////////////////////////

  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    ///////////////////////////////////////
    if (mem->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats, lands_);
      have_stat_update = true;
    }
    ///////////////////////////////////////
    mutex_.Lock();
  }

  //if (have_stat_update && current->UpdateStats(stats)) {
  //  MaybeScheduleCompaction();
  //}
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

///////////////////////////////////////
void DBImpl::Scan(const ReadOptions& options, 
                  const std::string& startKey, 
                  int length, 
                  std::vector<std::pair<std::string, std::string>>& kv) {
  //Status s;
  // 不要在这里加锁，NewIterator中也会加锁，这里加锁的话就执行不下去了
  //MutexLock l(&mutex_);
  Iterator* dbIter = NewIterator(options);
  dbIter->Seek(startKey);
  while (dbIter->Valid() && (length > 0)) { 
    kv.push_back(std::make_pair(dbIter->key().ToString(), dbIter->value().ToString()));
    dbIter->Next(); //2021_10_11 陈冠中添
    length --;
  }
  delete dbIter;
}
///////////////////////////////////////

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
