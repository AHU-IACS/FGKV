// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

// ///////////////////////////////////////
// #include "util/fg_stats.h"
// extern FG_Stats fg_stats;
// ///////////////////////////////////////

namespace leveldb {
///////////////////////////////////////
uint64_t Table::read_block_stats_[8][3] = {0};
// // FILE* Table::read_block_per_time_[8] = {0};
// // FILE* Table::read_block_per_time_ = fopen("/home/chen/L4_GET.txt", "w+");
// // Table::read_block_per_time_ = fopen("/home/chen/L4_GET.txt", "w+");
// FILE* Table::l0_read_block_per_time_ = fopen("/home/chen/FG_L0_GET.txt", "w+");
// FILE* Table::l1_read_block_per_time_ = fopen("/home/chen/FG_L1_GET.txt", "w+");
// FILE* Table::l2_read_block_per_time_ = fopen("/home/chen/FG_L2_GET.txt", "w+");
// FILE* Table::l3_read_block_per_time_ = fopen("/home/chen/FG_L3_GET.txt", "w+");
// FILE* Table::l4_read_block_per_time_ = fopen("/home/chen/FG_L4_GET.txt", "w+");
// FILE* Table::l5_read_block_per_time_ = fopen("/home/chen/FG_L5_GET.txt", "w+");
// FILE* Table::l6_read_block_per_time_ = fopen("/home/chen/FG_L6_GET.txt", "w+");
///////////////////////////////////////

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    delete index_block;
  }

  return s;
}

///////////////////////////////////////
Status Table::FG_Open(const Options& options,
                      RandomAccessFile* file,
                      uint64_t size,
                      Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  {
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->index_block = NULL;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  if (*table != NULL)
    return Status::OK();

}
///////////////////////////////////////

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
///////////////////////////////////////
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value,
                             void* arg2, 
                             int arg3) {
///////////////////////////////////////
  Table* table = reinterpret_cast<Table*>(arg);
  LevelAndStage* lands = reinterpret_cast<LevelAndStage*>(arg2);
  //bool* rw = reinterpret_cast<bool*>(arg3);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  //// <FG_STATS>
  uint64_t br_start = Env::Default()->NowMicros();

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        
        //// <fg_stats>
        (arg3 >= 7) ? (table->fg_stats_->block_reader_w_read_block_cache_count += 1) 
                    : (table->fg_stats_->block_reader_r_read_block_cache_count += 1);

        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {

        //// <fg_stats>
        uint64_t br_rb_start = Env::Default()->NowMicros();

        ///////////////////////////////////////
        s = ReadBlock(table->rep_->file, options, handle, &contents, 
                      // Table::l0_read_block_per_time_,
                      // Table::l1_read_block_per_time_,
                      // Table::l2_read_block_per_time_,
                      // Table::l3_read_block_per_time_,
                      // Table::l4_read_block_per_time_,
                      // Table::l5_read_block_per_time_, 
                      // Table::l6_read_block_per_time_, 
                      Table::read_block_stats_, arg3, lands);
        ///////////////////////////////////////

        //// <FG_STATS>
        if (arg3 >= 7) { 
          table->fg_stats_->block_reader_w_read_block_count += 1;
          table->fg_stats_->block_reader_w_read_block_time += Env::Default()->NowMicros() - br_rb_start;
        } else {
          table->fg_stats_->block_reader_r_read_block_count += 1;
          table->fg_stats_->block_reader_r_read_block_time += Env::Default()->NowMicros() - br_rb_start;
        }
        

        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {

      //// <fg_stats>
      uint64_t br_rb_start = Env::Default()->NowMicros();
      
        ///////////////////////////////////////
        s = ReadBlock(table->rep_->file, options, handle, &contents, 
                      // Table::l0_read_block_per_time_,
                      // Table::l1_read_block_per_time_,
                      // Table::l2_read_block_per_time_,
                      // Table::l3_read_block_per_time_,
                      // Table::l4_read_block_per_time_,
                      // Table::l5_read_block_per_time_, 
                      // Table::l6_read_block_per_time_, 
                      Table::read_block_stats_, arg3, lands);
        ///////////////////////////////////////

      //// <FG_STATS>
      if (arg3 > 7) { 
        table->fg_stats_->block_reader_w_read_block_count += 1;
        table->fg_stats_->block_reader_w_read_block_time += Env::Default()->NowMicros() - br_rb_start;
      } else {
        table->fg_stats_->block_reader_r_read_block_count += 1;
        table->fg_stats_->block_reader_r_read_block_time += Env::Default()->NowMicros() - br_rb_start;
      }
      
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }

  //// <FG_STATS>
  if (arg3 >= 7) { 
    table->fg_stats_->block_reader_w_count += 1;
    table->fg_stats_->block_reader_w_time += Env::Default()->NowMicros() - br_start;
  } else { 
    table->fg_stats_->block_reader_r_count += 1;
    table->fg_stats_->block_reader_r_time += Env::Default()->NowMicros() - br_start;
  }
  

  return iter;
}

///////////////////////////////////////
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options, NULL);
}
///////////////////////////////////////

///////////////////////////////////////
Status Table::InternalGet(const ReadOptions& options, 
                          int level, 
                          const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&), 
                          const Slice& blockHandle, 
                          std::vector<uint8_t>* fmd_stage, 
                          LevelAndStage* lands) {

  // <FG_STATS>
  fg_stats_->internal_get_count += 1;
  
  Status s;
  FilterBlockReader* filter = rep_->filter;
  BlockHandle handle;
  std::string handleString = blockHandle.ToString();
  Slice sliceHandleString = Slice(handleString);

  //// <FG_STATS>
  uint64_t itgt_start = Env::Default()->NowMicros();

  if (filter != NULL &&
     handle.DecodeFrom(&sliceHandleString).ok() &&
     !filter->KeyMayMatch(handle.offset(), k)) {
   // Not found
  } else {

    ///////////////////////////////////////
    Iterator* block_iter = BlockReader(this, options, blockHandle, lands, level);
    ///////////////////////////////////////
    block_iter->Seek(k);

    //// <FG_STATS>
    fg_stats_->internal_get_seek_data_block_time += Env::Default()->NowMicros() - itgt_start;

    if (block_iter->Valid()) {
      (*saver)(arg, block_iter->key(), block_iter->value());
    }
    s = block_iter->status();
    delete block_iter;
  }

  //// <FG_STATS>
  fg_stats_->internal_get_time += Env::Default()->NowMicros() - itgt_start;

  return s;
}
///////////////////////////////////////


uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
