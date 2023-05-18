#include "table/iterator_merger.h"

namespace leveldb{

class IteratorMerger : public Iterator{
 public:
    IteratorMerger(const Comparator* comparator, Iterator* iter1, Iterator* iter2)
            :comparator_(comparator), current_(NULL) {
                iter_[0] = iter1;
                iter_[1] = iter2;
    }

    virtual ~IteratorMerger(){}
    
    // 要求第二个迭代器有效。
    virtual bool Valid() const {
        return iter_[1]->Valid();
    }

    virtual void SeekToFirst() {
        // 需要保证iter_[0]是有效的，
        // SeekToFirst只能在input[1]的某个sstable刚开始进行遍历的时候调用
        assert(iter_[0]->Valid());
        iter_[1]->SeekToFirst();
        FindSmallest();
    }

    // 因为IteratorMerger的有效性和iter_[1]一致，所以它的最后一个记录就是
    // iter_[1]的最后一个记录
    virtual void SeekToLast() {
        iter_[1]->SeekToLast();
        current_ = iter_[1];
    }

    virtual void Seek(const Slice& target) {
        for (int i = 0; i < 2; i++) {
            iter_[i]->Seek(target);
        }
        FindSmallest();
    }

    virtual void Next() {
        assert(Valid());
        for (int i = 0; i < 2; i++) {
            Iterator* child = iter_[i];
            if (child != current_) {
                child->Seek(key());
                if (child->Valid() &&
                    comparator_->Compare(key(), child->key()) == 0) {
                    child->Next();
                }
            }
        }
        current_->Next();
        FindSmallest();
    }

    virtual void Prev() {
        assert(Valid());
        for (int i = 0; i < 2; i++) {
            Iterator* child = iter_[i];
            if (child != current_) {
                child->Seek(key());
                if (child->Valid()) {
                    // Child is at first entry >= key().  Step back one to be < key()
                    child->Prev();
                } else {
                    // Child has no entries >= key().  Position at last entry.
                    child->SeekToLast();
                }
            }
        }
        current_->Prev();
        FindLargest();
    }

    virtual Slice key() const {
        assert(Valid());
        return current_->key();
    }

    virtual Slice value() const {
        assert(Valid());
        return current_->value();
    }

    virtual Status status() const {
        Status status;
        for (int i = 0; i < 2; i++) {
            status = iter_[i]->status();
            if (!status.ok()) {
                break;
            }
        }
        return status;
    }

 private:
    void FindSmallest();
    void FindLargest();

    const Comparator* comparator_;
    bool isContinue_;
    Iterator* iter_[2];
    Iterator* current_;
    
};

void IteratorMerger::FindSmallest(){
    Iterator* smallest = NULL;
    for (int i = 0; i < 2; i++) {
        Iterator* child = iter_[i];
        if (child->Valid()) {
            if (smallest == NULL) {
                smallest = child;
            } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}

void IteratorMerger::FindLargest(){
    Iterator* largest = NULL;
    for (int i = 1; i >= 0; i--) {
        Iterator* child = iter_[i];
        if (child->Valid()) {
            if (largest == NULL) {
                largest = child;
            } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                largest = child;
            }
        }
    }
    current_ = largest;
}

Iterator* NewIteratorMerger(const Comparator* comparator, 
        Iterator* iter1, Iterator* iter2){
    return new IteratorMerger(comparator, iter1, iter2);
}

}