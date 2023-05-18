// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

///////////////////////////////////////
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta, 
                  FG_Stats* fg_stats) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  
  //std::string dbpath = std::string("/home/chen/ssd/") + file_dir[meta->number % 10];

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    ///////////////////////////////////////
    // 要把空的validBlock指针传进去
    TableBuilder* builder = new TableBuilder(options, 0/*全按L0算*/, file, NULL, meta->bim_, NULL, fg_stats);
    ///////////////////////////////////////
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      ///////////////////////////////////////
      // 使用创建根表的Add
      builder->RootAdd(key, iter->value(), 0);
      ///////////////////////////////////////
    }

    // Finish and check for builder errors
    if (s.ok()) {
    	///////////////////////////////////////
    	// 使用创建根表的Finish
    	s = builder->RootFinish(0);
    	///////////////////////////////////////
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    // if (s.ok()) {
    //   // Verify that the table is usable
    //   Iterator* it = table_cache->NewIterator(ReadOptions(),
    //                                           meta->number,
    //                                           meta->file_size);
    //   s = it->status();
    //   delete it;
    // }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}
///////////////////////////////////////

}  // namespace leveldb
