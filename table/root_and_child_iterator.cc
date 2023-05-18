#include <table/root_and_child_iterator.h>
#include "table/iterator_wrapper.h"

namespace leveldb{
class Table;

//typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class RootAndChildIterator : public Iterator{
 public:
    RootAndChildIterator(const ReadOptions& options, 
                        const InternalKeyComparator& icmp,
                        std::vector<Table*> table, 
                        std::vector<BlockInfo*>* chosenBlock)
            :   options_(options),
                icmp_(icmp),
                table_(table),
                chosen_block_(chosenBlock),
                block_iter_(NULL),
                index_(0) { }


    virtual ~RootAndChildIterator() { };

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
    std::vector<BlockInfo*>* chosen_block_;
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


void RootAndChildIterator::Seek(const Slice& target){
    uint32_t left = 0;
    uint32_t right = chosen_block_->size() - 1;
    while (left < right) {
      uint32_t mid = (left + right) / 2;
      Slice mid_key((*chosen_block_)[mid]->index_key);
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

void RootAndChildIterator::SeekToFirst(){
    index_ = 0;
    InitBlock();
    if (block_iter_.iter() != NULL) block_iter_.SeekToFirst();
    SkipEmptyBlocksForward();
}

void RootAndChildIterator::SeekToLast(){
    index_ = chosen_block_->size() - 1;
    InitBlock();
    if (block_iter_.iter() != NULL) block_iter_.SeekToLast();
    SkipEmptyBlocksBackward();
}

void RootAndChildIterator::Next(){
    assert(Valid());
    block_iter_.Next();
    SkipEmptyBlocksForward();
}

void RootAndChildIterator::Prev(){
    assert(Valid());
    block_iter_.Prev();
    SkipEmptyBlocksBackward();
}


void RootAndChildIterator::InitBlock(){
    if(index_ >= chosen_block_->size() || index_ < 0){
        SetBlockIterator(NULL);
    }else{
        Slice handle = (*chosen_block_)[index_]->block_handle;
        // 已更改，用last_index_指示上一次的block
        if (block_iter_.iter() != NULL && (index_ == last_index_)) {
            // block_iter_ is already constructed with this iterator, so
            // no need to change anything
        } else {
        	Table* t = table_[(*chosen_block_)[index_]->file];
            Iterator* iter = Table::BlockReader(t, options_, handle, NULL, 7);
            //Iterator* iter = (*table_)[(*chosen_block_)[index_].file]->BlockReader(this, options_, handle);
            //block_handle_.assign(handle.data(), handle.size());
            last_index_ = index_;    // 2021_1_24
            SetBlockIterator(iter);
        }
    }
}

void RootAndChildIterator::SetBlockIterator(Iterator* block_iter) {
    if (block_iter_.iter() != NULL) SaveError(block_iter_.status());
    block_iter_.Set(block_iter);
}

void RootAndChildIterator::SkipEmptyBlocksForward(){
    while (block_iter_.iter() == NULL || !block_iter_.Valid()) {
        // Move to next block
        if (index_ >= chosen_block_->size() || index_ < 0) {
            SetBlockIterator(NULL);
            return;
        }
        index_++;
        InitBlock();
        if (block_iter_.iter() != NULL) block_iter_.SeekToFirst();
    }
}

void RootAndChildIterator::SkipEmptyBlocksBackward(){
    while (block_iter_.iter() == NULL || !block_iter_.Valid()) {
        // Move to next block
        if (index_ >= chosen_block_->size() || index_ < 0) {
            SetBlockIterator(NULL);
            return;
        }
        index_--;
        InitBlock();
        if (block_iter_.iter() != NULL) block_iter_.SeekToLast();
    }
}

Iterator* NewRootAndChildIterator(
    const ReadOptions& options,
	const InternalKeyComparator& icmp,
	std::vector<Table*> table,
	std::vector<BlockInfo*>* chosenBlock) {
	return new RootAndChildIterator(options, icmp, table, chosenBlock);
}

}

