// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

///////////////////////////////////////
#include "table/region_info_manager.h"
///////////////////////////////////////

namespace leveldb {

class VersionSet;

class RegionInfoManager;

///////////////////////////////////////
struct PatchMetaData{
	uint64_t number;
	uint64_t file_size;

  int total_regions_count;
  int invalid_regions_count;
  uint64_t invalid_size;
  bool is_valid;

	PatchMetaData():file_size(0), total_regions_count(0), invalid_regions_count(0), invalid_size(0), is_valid(true) {}
};
///////////////////////////////////////

///////////////////////////////////////
struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number; 
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  std::vector<PatchMetaData> patch_file;
  mutable RegionInfoManager* rim_;
  mutable RegionInfoManager* bkp_bim_;

  std::vector<uint8_t> fmd_stage_;

  FileMetaData() : refs(0), allowed_seeks(1 << 30), rim_(NULL), bkp_bim_(NULL) { 
    fmd_stage_.push_back(0);
  }
  FileMetaData(const FileMetaData& f) : refs(f.refs), allowed_seeks(f.allowed_seeks), 
              number(f.number), file_size(f.file_size), smallest(f.smallest), 
              largest(f.largest), rim_(f.rim_), bkp_bim_(f.bkp_bim_) { 
    fmd_stage_.assign(f.fmd_stage_.begin(), f.fmd_stage_.end());
    patch_file.assign(f.patch_file.begin(), f.patch_file.end());
    f.rim_ = NULL;
    f.bkp_bim_ = NULL;
  }

  ~FileMetaData() { 
    // assert(rim_->refs > 0);
    // rim_->refs--;
    // if (rim_->refs <= 0) { 
    //   delete rim_;
    //   //rim_ = NULL;
    // }
    //std::vector<uint8_t>().swap(fmd_stage_);
    std::vector<PatchMetaData>().swap(patch_file);
    if (rim_ != NULL) {
      rim_->refs--;
      if (rim_->refs <= 0) { 
        delete rim_;
        //rim_ = NULL;
      }
      rim_ = NULL;
    }
    if (bkp_bim_ != NULL) {
      bkp_bim_->refs--;
      if (bkp_bim_->refs <= 0) { 
        delete bkp_bim_;
        //rim_ = NULL;
      }
      bkp_bim_ = NULL;
    }
  }

};
///////////////////////////////////////

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  ///////////////////////////////////////
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest,
               // 添加参数
               //RegionInfoManager** rIM,
               RegionInfoManager* rIM,
               const std::vector<PatchMetaData>& patchFile, 
               const std::vector<uint8_t>& fmd_stage) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    //f.rim_ = (*rIM);
    //(*rIM) = NULL;
    //f.refs = rIM->refs;
    f.rim_ = rIM;
    f.rim_->refs++;
    f.patch_file.assign(patchFile.begin(), patchFile.end());
    //f.fmd_stage_.assign(fmd_stage.begin(), fmd_stage.end());
    //rIM->refs--;
    new_files_.push_back(std::make_pair(level, f));
  }
  ///////////////////////////////////////

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
