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
#include "table/block_info_manager.h"
///////////////////////////////////////

namespace leveldb {

class VersionSet;

class BlockInfoManager;

///////////////////////////////////////
struct ChildMetaData{
	uint64_t number;
	uint64_t file_size;

  int total_blocks_count;
  int invalid_blocks_count;
  uint64_t invalid_size;
  // 该补丁是否还有无效块
  bool is_valid;

	ChildMetaData():file_size(0), total_blocks_count(0), invalid_blocks_count(0), invalid_size(0), is_valid(true) {}
};
///////////////////////////////////////

///////////////////////////////////////
struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;    // 根表的文件号
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  std::vector<ChildMetaData> child_file;
  mutable BlockInfoManager* bim_;
  mutable BlockInfoManager* bkp_bim_;

  // 元数据处于的阶段
  // 0是普通阶段
  std::vector<uint8_t> fmd_stage_;

  FileMetaData() : refs(0), allowed_seeks(1 << 30), bim_(NULL), bkp_bim_(NULL) { 
    fmd_stage_.push_back(0);
  }
  FileMetaData(const FileMetaData& f) : refs(f.refs), allowed_seeks(f.allowed_seeks), 
              number(f.number), file_size(f.file_size), smallest(f.smallest), 
              largest(f.largest), bim_(f.bim_), bkp_bim_(f.bkp_bim_) { 
    fmd_stage_.assign(f.fmd_stage_.begin(), f.fmd_stage_.end());
    child_file.assign(f.child_file.begin(), f.child_file.end());
    f.bim_ = NULL;
    f.bkp_bim_ = NULL;
  }

  ~FileMetaData() { 
    // assert(bim_->refs > 0);
    // bim_->refs--;
    // if (bim_->refs <= 0) { 
    //   delete bim_;
    //   //bim_ = NULL;
    // }
    //std::vector<uint8_t>().swap(fmd_stage_);
    std::vector<ChildMetaData>().swap(child_file);
    if (bim_ != NULL) {
      bim_->refs--;
      if (bim_->refs <= 0) { 
        delete bim_;
        //bim_ = NULL;
      }
      bim_ = NULL;
    }
    if (bkp_bim_ != NULL) {
      bkp_bim_->refs--;
      if (bkp_bim_->refs <= 0) { 
        delete bkp_bim_;
        //bim_ = NULL;
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
               //BlockInfoManager** bIM,
               BlockInfoManager* bIM,
               const std::vector<ChildMetaData>& childFile, 
               const std::vector<uint8_t>& fmd_stage) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    // 给f的valid_block赋值
    //f.bim_ = (*bIM);
    //(*bIM) = NULL;
    //f.refs = bIM->refs;
    f.bim_ = bIM;
    f.bim_->refs++;
    // 给f的child_file赋值
    f.child_file.assign(childFile.begin(), childFile.end());
    //f.fmd_stage_.assign(fmd_stage.begin(), fmd_stage.end());
    //bIM->refs--;
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
