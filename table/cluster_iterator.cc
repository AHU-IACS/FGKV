// 作者：陈冠中

#include <table/cluster_iterator.h>
#include "table/iterator_wrapper.h"

namespace leveldb{
class Table;

//typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class ClusterIterator : public Iterator{
 public:
    ClusterIterator(const ReadOptions& options, 
                        const InternalKeyComparator& icmp,
                        std::vector<Table*> table, 
                        std::vector<RegionInfo*>* chosenRegion)
            :   options_(options),
                icmp_(icmp),
                table_(table),
                chosen_region_(chosenRegion),
                block_iter_(NULL),
                index_(0) { }


    virtual ~ClusterIterator() { };

    virtual void Seek(const Slice& target);
    virtual void SeekToFirst();
    virtual void SeekToLast();
    virtual void Next();
    virtual void Prev();
    
    virtual bool Valid() const {
        return block_iter_.Valid();
    }
    
    virtual Slice key() const {
        assert(Valid());
        return block_iter_.key();
    }
    Slice value() const {
        assert(Valid());
        return block_iter_.value();
    }
    virtual Status status() const {
        // It'd be nice if status() returned a const Status& instead of a Status
        if (block_iter_.iter() != NULL && !block_iter_.status().ok()) {
            return block_iter_.status();
        }else{ return status_;}
    }

 private:
    const ReadOptions options_;
    const InternalKeyComparator icmp_;
    Status status_;
    std::vector<Table*> table_;
    std::vector<RegionInfo*>* chosen_region_;
    IteratorWrapper block_iter_;

    uint64_t index_;
    uint64_t last_index_;    // 2021_1_24
    //std::string block_handle_;

    void SaveError(const Status& s) {
        if (status_.ok() && !s.ok()) status_ = s;
    }
    void InitBlock();
    void SetBlockIterator(Iterator* block_iter);
    void SkipEmptyBlocksForward();
    void SkipEmptyBlocksBackward();

};


void ClusterIterator::Seek(const Slice& target){
    uint32_t left = 0;
    uint32_t right = chosen_region_->size() - 1;
    while (left < right) {
      uint32_t mid = (left + right) / 2;
      Slice mid_key((*chosen_region_)[mid]->index_key);
      if (icmp_.Compare(mid_key, target) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    index_ = left;
    InitBlock();
    if (block_iter_.iter() != NULL) block_iter_.Seek(target);
    SkipEmptyBlocksForward();
}

void ClusterIterator::SeekToFirst(){
    index_ = 0;
    InitBlock();
    if (block_iter_.iter() != NULL) block_iter_.SeekToFirst();
    SkipEmptyBlocksForward();
}

void ClusterIterator::SeekToLast(){
    index_ = chosen_region_->size() - 1;
    InitBlock();
    if (block_iter_.iter() != NULL) block_iter_.SeekToLast();
    SkipEmptyBlocksBackward();
}

void ClusterIterator::Next(){
    assert(Valid());
    block_iter_.Next();
    SkipEmptyBlocksForward();
}

void ClusterIterator::Prev(){
    assert(Valid());
    block_iter_.Prev();
    SkipEmptyBlocksBackward();
}


void ClusterIterator::InitBlock(){
    if(index_ >= chosen_region_->size() || index_ < 0){
        SetBlockIterator(NULL);
    }else{
        Slice handle = (*chosen_region_)[index_]->block_handle;
        // 2021_1_24
        // 已更改，用last_index_指示上一次的region
        if (block_iter_.iter() != NULL && (index_ == last_index_)) {
            // block_iter_ is already constructed with this iterator, so
            // no need to change anything
        } else {
        	Table* t = table_[(*chosen_region_)[index_]->file];
            Iterator* iter = Table::BlockReader(t, options_, handle, NULL, 7);
            //Iterator* iter = (*table_)[(*chosen_region_)[index_].file]->BlockReader(this, options_, handle);
            //block_handle_.assign(handle.data(), handle.size());
            last_index_ = index_;    // 2021_1_24
            SetBlockIterator(iter);
        }
    }
}

void ClusterIterator::SetBlockIterator(Iterator* block_iter) {
    if (block_iter_.iter() != NULL) SaveError(block_iter_.status());
    block_iter_.Set(block_iter);
}

void ClusterIterator::SkipEmptyBlocksForward(){
    while (block_iter_.iter() == NULL || !block_iter_.Valid()) {
        // Move to next block
        if (index_ >= chosen_region_->size() || index_ < 0) {
            SetBlockIterator(NULL);
            return;
        }
        index_++;
        InitBlock();
        if (block_iter_.iter() != NULL) block_iter_.SeekToFirst();
    }
}

void ClusterIterator::SkipEmptyBlocksBackward(){
    while (block_iter_.iter() == NULL || !block_iter_.Valid()) {
        // Move to next block
        if (index_ >= chosen_region_->size() || index_ < 0) {
            SetBlockIterator(NULL);
            return;
        }
        index_--;
        InitBlock();
        if (block_iter_.iter() != NULL) block_iter_.SeekToLast();
    }
}

Iterator* NewClusterIterator(
    const ReadOptions& options,
	const InternalKeyComparator& icmp,
	std::vector<Table*> table,
	std::vector<RegionInfo*>* chosenRegion) {
	return new ClusterIterator(options, icmp, table, chosenRegion);
}

}

