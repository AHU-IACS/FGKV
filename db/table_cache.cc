// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

// ///////////////////////////////////////
// #include "util/fg_stats.h"
// extern FG_Stats fg_stats;
// ///////////////////////////////////////

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

///////////////////////////////////////
TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries, 
                       FG_Stats* fg_stats)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)), 
      fg_stats_(fg_stats) {
}
///////////////////////////////////////

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  //// <FG_STATS>
  uint64_t ft_start = env_->NowMicros();
  
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {

    //std::string dbpath = std::string("/home/chen/ssd/") + file_dir[file_number % 10];

    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::FG_Open(*options_, file, file_size, &table);
      //s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      table->SetFGStats(fg_stats_);
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }

  //// <FG_STATS>
  fg_stats_->find_table_time += env_->NowMicros() - ft_start;
  
  return s;
}

///////////////////////////////////////
Status TableCache::FG_FindIndexKey(FileMetaData* f, const Slice& k, BlockInfo* b) { 
  
  //// <FG_STATS>
  uint64_t fi_start = env_->NowMicros();

  if (f == NULL) { 
    return Status::Corruption("FileMetaData Not Found");
  }
  std::vector<BlockInfo*>* validBlock = &(f->bim_->fg_valid_block_);
  uint64_t left = 0;
  uint64_t right = validBlock->size() - 1;
  while (left < right) {
    uint64_t mid = (left + right) / 2;
    Slice mid_key((*validBlock)[mid]->index_key);
    if (options_->comparator->Compare(mid_key, k) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  b->index_key = (*validBlock)[left]->index_key;
  b->file = (*validBlock)[left]->file;
  b->block_handle = (*validBlock)[left]->block_handle;

  //// <FG_STATS>
  fg_stats_->find_index_key_time += env_->NowMicros() - fi_start;

  return Status::OK();

}
///////////////////////////////////////

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

///////////////////////////////////////
// k是InternalKey
Status TableCache::Get(const ReadOptions& options,
                       int level, 
                       FileMetaData* f,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&), 
                       LevelAndStage* lands) {
  
  //// <FG_STATS>
  fg_stats_->table_cache_get_count += 1;
  uint64_t tcg_start = Env::Default()->NowMicros();

  Cache::Handle* handle = NULL;
  BlockInfo bInfo;
  Status s = FG_FindIndexKey(f, k, &bInfo);

  if (s.ok()) { 
    uint64_t fileNumber = f->child_file[bInfo.file].number;
    uint64_t fileSize = f->child_file[bInfo.file].file_size;
    s = FindTable(fileNumber, fileSize, &handle);
    if (s.ok()) {
      Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
      //Slice blockHandle = Slice(bInfo.block_handle);

      // //// <FG_STATS>
      // uint64_t tcg_itg = env_->NowMicros();
      s = t->InternalGet(options, level, k, arg, saver, Slice(bInfo.block_handle), &(f->fmd_stage_), lands);
      // tcg_itg = env_->NowMicros() - tcg_itg;
      // if (tcg_itg > 300) { 
      //    if (f->isCompacting) { 
      //      printf("Com \n");
      //    } else { 
      //      printf("NoC \n");
      //    }
      // }

      cache_->Release(handle);
    }
  }

  //// <FG_STATS>
  fg_stats_->table_cache_get_time += Env::Default()->NowMicros() - tcg_start;
  
  return s;
}
///////////////////////////////////////

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

///////////////////////////////////////
Iterator* TableCache::NewMixedIterator(
                      const ReadOptions& options,
                      const InternalKeyComparator& icmp,
									    std::vector<ChildMetaData>* childFile, 
									    std::vector<BlockInfo*>* chosenBlock) {
  // std::vector<Cache::Handle*> handle;
  // //std::vector<Cache::Handle**> handle; ?
  // std::vector<Table*> table;
  // for(int i = 0; i < childFile->size(); i++){
  //   handle.push_back(reinterpret_cast<Cache::Handle*>(NULL));
  //   Status s=FindTable((*childFile)[i].number, (*childFile)[i].file_size, &handle[i]);
  //   if(s.ok()){
  //     table.push_back(reinterpret_cast<TableAndFile*>(cache_->Value(handle[i]))->table);
  //   }
  // }

  std::vector<Cache::Handle*> handle;
  //std::vector<Cache::Handle**> handle; ?
  std::vector<Table*> table;
  for(int i = 0; i < childFile->size(); i++){
    if ((*childFile)[i].is_valid) {
      Cache::Handle* h = NULL;
      Status s = FindTable((*childFile)[i].number, (*childFile)[i].file_size, &h);
      if (!s.ok()) {
        return NewErrorIterator(s);
      }
      table.push_back(reinterpret_cast<TableAndFile*>(cache_->Value(h))->table);
      handle.push_back(h);
    } else {
      table.push_back(NULL);
    }
  }

  Iterator* result = NewRootAndChildIterator(options, icmp, table, chosenBlock);
  
  for(int i = 0; i < handle.size(); i++) {
    result->RegisterCleanup(&UnrefEntry, cache_, handle[i]);
  }

  return result;

}
///////////////////////////////////////

}  // namespace leveldb
