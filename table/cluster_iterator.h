// 作者：陈冠中

#ifndef _FGKV_CLUSTER_ITERATOR_H_
#define _FGKV_CLUSTER_ITERATOR_H_

#include <vector>
#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "db/dbformat.h"

namespace leveldb {

struct ReadOptions;

///////////////////////////////////////
extern Iterator* NewClusterIterator(
    const ReadOptions& options,
	const InternalKeyComparator& icmp,
	std::vector<Table*> table,
	std::vector<RegionInfo*>* chosenRegion);
///////////////////////////////////////

}  // namespace leveldb

#endif  //_FGKV_CLUSTER_ITERATOR_H_
