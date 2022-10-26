// 作者：陈冠中

#ifndef _FGKV_REGION_INFO_MANAGER_
#define _FGKV_REGION_INFO_MANAGER_

#include <string>
#include <vector>


namespace leveldb { 

class TableCache;
class TableBuilder;

struct RegionInfo {
	std::string index_key;
	int file;
	std::string block_handle;
    short refs;
	RegionInfo () : file(0), refs(0) { }
	// RegionInfo (const RegionInfo& binfo) {
	// 	this->index_key = binfo.index_key;
	// 	this->file = binfo.file;
	// 	this->block_handle = binfo.block_handle;
	// }
};

class RegionInfoManager {
 public:
    RegionInfoManager() : refs(0) { 
        fg_valid_region_.resize(0);
    }
    RegionInfoManager(RegionInfoManager& rIM) : refs(rIM.refs) {
        fg_valid_region_.assign(rIM.fg_valid_region_.begin(), rIM.fg_valid_region_.end());
    }
    ~RegionInfoManager() { 
        for (int i = 0; i < fg_valid_region_.size(); i++) { 
            fg_valid_region_[i]->refs--;
            if (fg_valid_region_[i]->refs <= 0)
                delete fg_valid_region_[i];
        }
        std::vector<RegionInfo*>().swap(fg_valid_region_);
    }

    std::vector<RegionInfo*>* ValidRegion() { 
        return &fg_valid_region_;
    }


    int refs;

 private:
    
    friend class TableCache;
    friend class TableBuilder;

    std::vector<RegionInfo*> fg_valid_region_;
};

}

#endif


