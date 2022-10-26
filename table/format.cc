// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

// ///////////////////////////////////////
// #include "util/fg_stats.h"
// extern FG_Stats fg_stats;
// ///////////////////////////////////////

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

// Status ReadBlock(RandomAccessFile* file,
//                  const ReadOptions& options,
//                  const BlockHandle& handle,
//                  BlockContents* result, 
//                  ///////////////////////////////////////
//                  FILE* l0_read_per_time,
//                  FILE* l1_read_per_time,
//                  FILE* l2_read_per_time,
//                  FILE* l3_read_per_time,
//                  FILE* l4_read_per_time, 
//                  FILE* l5_read_per_time,
//                  FILE* l6_read_per_time,
//                  uint64_t (*read_block_stats)[3],
//                  //FG_Stats* fg_stats, 
//                  // uint64_t* read_block_r_file_read_time, 
//                  // uint64_t* read_block_r_file_read_size, 
//                  // uint64_t* read_block_w_file_read_time, 
//                  // uint64_t* read_block_w_file_read_size, 
//                  int level, 
//                  std::vector<uint8_t>* fmd_stage
//                  ) {
//                  ///////////////////////////////////////
  
//   FILE* out_file;
//   switch (level)
//   {
//   case 0:
//     out_file =  l0_read_per_time;
//     break;
//   case 1:
//     out_file =  l1_read_per_time;
//     break;
//   case 2:
//     out_file =  l2_read_per_time;
//     break;
//   case 3:
//     out_file =  l3_read_per_time;
//     break;
//   case 4:
//     out_file =  l4_read_per_time;
//     break;
//   case 5:
//     out_file =  l5_read_per_time;
//     break;
//   case 6:
//     out_file =  l6_read_per_time;
//     break;
  
//   default:
//     out_file = NULL;
//     break;
//   }
//   //// <FG_STATS>
//   uint64_t rb_start = Env::Default()->NowMicros();
//   // if (fg_stats != NULL) { 
//   //   fg_stats->block_reader_read_block_count++;
//   // }
  
//   result->data = Slice();
//   result->cachable = false;
//   result->heap_allocated = false;

//   // Read the block contents as well as the type/crc footer.
//   // See table_builder.cc for the code that built this structure.
//   size_t n = static_cast<size_t>(handle.size());
//   char* buf = new char[n + kBlockTrailerSize];
//   Slice contents;

//   //// <FG_STATS>
//   uint64_t rb_fr_start = Env::Default()->NowMicros();
//   // 在读之前先输出fmd状态路径
//   if (read_block_stats != NULL && out_file != NULL) { 
//     for (int i = 0; i < (*fmd_stage).size(); i ++) { 
//       fprintf(out_file, "--%d", (*fmd_stage)[i]);
//     }
//     fprintf(out_file, "\n");
//   }
//   Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
//   //// <FG_STATS>
//   if (read_block_stats != NULL) { 
//     uint64_t fr_time = Env::Default()->NowMicros() - rb_fr_start;
//     read_block_stats[level][0] += 1;
//     read_block_stats[level][1] += fr_time;
//     read_block_stats[level][2] += n + kBlockTrailerSize;

//     if (out_file != NULL) { 
//       // fprintf(out_file, "%lld\n", static_cast<unsigned long long>(fr_time));
//       if (fr_time > 100) { 
//         fprintf(out_file, "!");
//         for (int i = 0; i < (*fmd_stage).size(); i ++) { 
//           fprintf(out_file, "--%d", (*fmd_stage)[i]);
//         }
//         fprintf(out_file, "\n");
//       }
//     }

//   }

//   if (!s.ok()) {
//     delete[] buf;
//     return s;
//   }
//   if (contents.size() != n + kBlockTrailerSize) {
//     delete[] buf;
//     return Status::Corruption("truncated block read");
//   }

//   // Check the crc of the type and the block contents
//   const char* data = contents.data();    // Pointer to where Read put the data
//   if (options.verify_checksums) {
//     const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
//     const uint32_t actual = crc32c::Value(data, n + 1);
//     if (actual != crc) {
//       delete[] buf;
//       s = Status::Corruption("block checksum mismatch");
//       return s;
//     }
//   }

//   switch (data[n]) {
//     case kNoCompression:
//       if (data != buf) {
//         // File implementation gave us pointer to some other data.
//         // Use it directly under the assumption that it will be live
//         // while the file is open.
//         delete[] buf;
//         result->data = Slice(data, n);
//         result->heap_allocated = false;
//         result->cachable = false;  // Do not double-cache
//       } else {
//         result->data = Slice(buf, n);
//         result->heap_allocated = true;
//         result->cachable = true;
//       }

//       // Ok
//       break;
//     case kSnappyCompression: {
//       size_t ulength = 0;
//       if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//         delete[] buf;
//         return Status::Corruption("corrupted compressed block contents");
//       }
//       char* ubuf = new char[ulength];
//       if (!port::Snappy_Uncompress(data, n, ubuf)) {
//         delete[] buf;
//         delete[] ubuf;
//         return Status::Corruption("corrupted compressed block contents");
//       }
//       delete[] buf;
//       result->data = Slice(ubuf, ulength);
//       result->heap_allocated = true;
//       result->cachable = true;
//       break;
//     }
//     default:
//       delete[] buf;
//       return Status::Corruption("bad block type");
//   }

//   // //// <FG_STATS>
//   // if (fg_stats != NULL) { 
//   //   fg_stats->block_reader_read_block_time += Env::Default()->NowMicros() - rb_start;
//   // }
//   // if (read_block_r_file_read_time != NULL) { 
//   //   if (rw) { 
//   //     *read_block_r_file_read_time += Env::Default()->NowMicros() - rb_fr_start;
//   //     *read_block_r_file_read_size += n + kBlockTrailerSize;
//   //   } else { 
//   //     *read_block_w_file_read_time += Env::Default()->NowMicros() - rb_fr_start;
//   //     *read_block_w_file_read_size += n + kBlockTrailerSize;
//   //   } 
//   // }
  
//   return Status::OK();

// }

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result, 
                 ///////////////////////////////////////
                //  FILE* l0_read_per_time,
                //  FILE* l1_read_per_time,
                //  FILE* l2_read_per_time,
                //  FILE* l3_read_per_time,
                //  FILE* l4_read_per_time, 
                //  FILE* l5_read_per_time,
                //  FILE* l6_read_per_time,
                 uint64_t (*read_block_stats)[3],
                //  FG_Stats* fg_stats, 
                //  uint64_t* read_block_r_file_read_time, 
                //  uint64_t* read_block_r_file_read_size, 
                //  uint64_t* read_block_w_file_read_time, 
                //  uint64_t* read_block_w_file_read_size, 
                 int level, 
                 LevelAndStage* lands
                 ) {
                 ///////////////////////////////////////
  
  // FILE* out_file;
  // switch (level)
  // {
  // case 0:
  //   out_file =  l0_read_per_time;
  //   break;
  // case 1:
  //   out_file =  l1_read_per_time;
  //   break;
  // case 2:
  //   out_file =  l2_read_per_time;
  //   break;
  // case 3:
  //   out_file =  l3_read_per_time;
  //   break;
  // case 4:
  //   out_file =  l4_read_per_time;
  //   break;
  // case 5:
  //   out_file =  l5_read_per_time;
  //   break;
  // case 6:
  //   out_file =  l6_read_per_time;
  //   break;
  
  // default:
  //   out_file = NULL;
  //   break;
  // }
  // //// <FG_STATS>
  // uint64_t rb_start = Env::Default()->NowMicros();
  // if (fg_stats != NULL) { 
  //   fg_stats->block_reader_read_block_count++;
  // }
  
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;

  //// <FG_STATS>
  uint64_t rb_fr_start = Env::Default()->NowMicros();
  // 在读之前先输出c_stage状态
  // if (lands->stage != NULL && read_block_stats != NULL && out_file != NULL) { 
  //   fprintf(out_file, "L%dS%d\n", lands->level, *(lands->stage));
  // }
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  //// <FG_STATS>
  if (read_block_stats != NULL) { 
    uint64_t fr_time = Env::Default()->NowMicros() - rb_fr_start;
    read_block_stats[level][0] += 1;
    read_block_stats[level][1] += fr_time;
    read_block_stats[level][2] += n + kBlockTrailerSize;

    // if (lands != NULL && lands->stage != NULL && out_file != NULL && fr_time > 100) { 
    //   // fprintf(out_file, "%lld\n", static_cast<unsigned long long>(fr_time));
    //   fprintf(out_file, "!L%dS%d--%lld\n", lands->level, *(lands->stage), static_cast<unsigned long long>(fr_time));
    //   //fprintf(out_file, "%lld!--%d\n", static_cast<unsigned long long>(fr_time), *c_stage);
    // }

  }

  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  // //// <FG_STATS>
  // if (fg_stats != NULL) { 
  //   fg_stats->block_reader_read_block_time += Env::Default()->NowMicros() - rb_start;
  // }
  // if (read_block_r_file_read_time != NULL) { 
  //   if (rw) { 
  //     *read_block_r_file_read_time += Env::Default()->NowMicros() - rb_fr_start;
  //     *read_block_r_file_read_size += n + kBlockTrailerSize;
  //   } else { 
  //     *read_block_w_file_read_time += Env::Default()->NowMicros() - rb_fr_start;
  //     *read_block_w_file_read_size += n + kBlockTrailerSize;
  //   } 
  // }
  
  return Status::OK();

}

}  // namespace leveldb
