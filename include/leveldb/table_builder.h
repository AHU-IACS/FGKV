// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <stdint.h>
#include "leveldb/options.h"
#include "leveldb/status.h"

///////////////////////////////////////
#include <vector>
#include "table/block_info_manager.h"
///////////////////////////////////////

///////////////////////////////////////
#include "util/fg_stats.h"
///////////////////////////////////////

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct ChildMetaData;

class TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  ///////////////////////////////////////
  TableBuilder(const Options& options, 
                int level, 
                WritableFile* file, 
                BlockInfoManager* srcBim, 
                BlockInfoManager* desBim, 
                // std::vector<int> invalidChild, 
                std::vector<ChildMetaData>* childFiles,
                FG_Stats* fg_stats);
  ///////////////////////////////////////

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  Status ChangeOptions(const Options& options);

  ///////////////////////////////////////
  // 更新valid_block
  void UpdateIndexEntry(int count);
  ///////////////////////////////////////

  ///////////////////////////////////////
  // 创建根表的Add
  void RootAdd(const Slice& key, const Slice& value, int count);
  ///////////////////////////////////////

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  ///////////////////////////////////////
  // 创建子表的Add，count指示这是该族的第几次compaction
  void Add(const Slice& key, const Slice& value, int count);
  ///////////////////////////////////////

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush();

  // Return non-ok iff some error has been detected.
  Status status() const;

  ///////////////////////////////////////
  // 根表的Finish
  Status RootFinish(int count);
  ///////////////////////////////////////

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  ///////////////////////////////////////
  Status Finish(int count);
  ///////////////////////////////////////

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon();

  // Number of calls to Add() so far.
  uint64_t NumEntries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const;

  ///////////////////////////////////////
  // int BlocksCount() {return des_valid_block_->size();}
  int flush_blocks_count;
  ///////////////////////////////////////

 private:
  bool ok() const { return status().ok(); }
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;

  ///////////////////////////////////////
  // 正在生成的sstable属于哪一层
  int level_;
  // 根据选中的block的内容构建sstable
  BlockInfoManager* src_bim_;
  BlockInfoManager* des_bim_;
  std::vector<ChildMetaData>* child_file_;
  std::vector<BlockInfo*>* src_valid_block_;
  std::vector<BlockInfo*>* des_valid_block_;
  uint64_t biter_;
  // 是否要拆分block
  bool split_;

  //// <FG_STATS>
  FG_Stats* fg_stats_;
  ///////////////////////////////////////

  // No copying allowed
  TableBuilder(const TableBuilder&);
  void operator=(const TableBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
