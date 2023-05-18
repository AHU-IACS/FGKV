// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include "leveldb/iterator.h"

///////////////////////////////////////
//#include "table/root_and_child_iterator.h"
#include <vector>
///////////////////////////////////////

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

///////////////////////////////////////
class RootAndChildIterator;
///////////////////////////////////////

///////////////////////////////////////
#include "util/fg_stats.h"
///////////////////////////////////////

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     RandomAccessFile* file,
                     uint64_t file_size,
                     Table** table);

  ///////////////////////////////////////
  static Status FG_Open(const Options& options,
                        RandomAccessFile* file,
                        uint64_t file_size,
                        Table** table);
  ///////////////////////////////////////

  ///////////////////////////////////////
  void SetFGStats(FG_Stats* fg_stats) {
    fg_stats_ = fg_stats;
  }
  ///////////////////////////////////////
  
  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

  ///////////////////////////////////////
  friend class RootAndChildIterator;

  // 第二维分别是 count time size
  // 第八行表示compact引发的
  static uint64_t read_block_stats_[8][3];

  // // static FILE* read_block_per_time_[8];
  // static FILE* l0_read_block_per_time_;
  // static FILE* l1_read_block_per_time_;
  // static FILE* l2_read_block_per_time_;
  // static FILE* l3_read_block_per_time_;
  // static FILE* l4_read_block_per_time_;
  // static FILE* l5_read_block_per_time_;
  // static FILE* l6_read_block_per_time_;
  ///////////////////////////////////////

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) { rep_ = rep; }

  ///////////////////////////////////////
  FG_Stats* fg_stats_;
  ///////////////////////////////////////

  ///////////////////////////////////////
  // int值7表示compact引发的，0-6表示Get引发的
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&, void*, int);
  ///////////////////////////////////////

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  friend class TableCache;
  Status InternalGet(
      const ReadOptions&,
      int level, 
      const Slice& key,
      void* arg,
      void (*handle_result)(void* arg, const Slice& k, const Slice& v), 
      ///////////////////////////////////////
      const Slice& BlockHandle, 
      std::vector<uint8_t>* fmd_stage, 
      LevelAndStage* lands
      ///////////////////////////////////////
      );


  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);

  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};



}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
