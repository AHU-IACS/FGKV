#ifndef _FGKV_BLOCK_INFO_MANAGER_
#define _FGKV_BLOCK_INFO_MANAGER_

#include <string>
#include <vector>


namespace leveldb { 

class TableCache;
class TableBuilder;

struct BlockInfo {
	std::string index_key;
	// block在本簇哪个文件中，每进行一次compaction，file+1
	int file;
	// 【block偏移量】【block大小】//2021_1_14添加
	std::string block_handle;
    short refs;
	BlockInfo () : file(0), refs(0) { }
	// BlockInfo (const BlockInfo& binfo) {
	// 	this->index_key = binfo.index_key;
	// 	this->file = binfo.file;
	// 	this->block_handle = binfo.block_handle;
	// }
};

// 新的BlockInfoManager对象必须得是new创建的
class BlockInfoManager {
 public:
    BlockInfoManager() : refs(0) { 
        fg_valid_block_.resize(0);
    }
    BlockInfoManager(BlockInfoManager& bIM) : refs(bIM.refs) {
        fg_valid_block_.assign(bIM.fg_valid_block_.begin(), bIM.fg_valid_block_.end());
    }
    ~BlockInfoManager() { 
        for (int i = 0; i < fg_valid_block_.size(); i++) { 
            fg_valid_block_[i]->refs--;
            if (fg_valid_block_[i]->refs <= 0)
                delete fg_valid_block_[i];
        }
        std::vector<BlockInfo*>().swap(fg_valid_block_);
    }

    std::vector<BlockInfo*>* ValidBlock() { 
        return &fg_valid_block_;
    }


    // 为了避免在读的时候进行compaction导致读不到正确的有效块索引
    // 读操作使refs++，添加一个引用计数，只有引用计数为0，才能delete
    int refs;

 private:
    // 指明遍历到哪个index
    
    friend class TableCache;
    friend class TableBuilder;

    std::vector<BlockInfo*> fg_valid_block_;
};

}

#endif


