#ifndef _CHEN_ROOT_AND_CHILD_ITERATOR_H_
#define _CHEN_ROOT_AND_CHILD_ITERATOR_H_

#include <vector>
#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "db/dbformat.h"

namespace leveldb {

struct ReadOptions;

///////////////////////////////////////
extern Iterator* NewRootAndChildIterator(
    const ReadOptions& options,
	const InternalKeyComparator& icmp,
	std::vector<Table*> table,
	std::vector<BlockInfo*>* chosenBlock);
///////////////////////////////////////

}  // namespace leveldb

#endif  //_CHEN_ROOT_AND_CHILD_ITERATOR_H_
