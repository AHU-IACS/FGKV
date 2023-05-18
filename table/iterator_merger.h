#ifndef _CHEN_ITERATOR_MERGER_H_
#define _CHEN_ITERATOR_MERGER_H_

#include "leveldb/iterator.h"
#include "leveldb/comparator.h"

namespace leveldb {

extern Iterator* NewIteratorMerger(
        const Comparator* comparator, 
        Iterator* iter1, 
        Iterator* iter2);

}  // namespace leveldb

#endif  // _CHEN_ITERATOR_MERGER_H_
