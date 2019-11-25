#pragma once

#include "../nodes/node.hh"
#include "local_io.hh"
#include "../messages/IOoperation.hh"
#include "../common/context_singleton.hh"
#include <string>
#include <mutex>
#include <map>

#include <iostream>
#include <cstdio>

#include <queue>

#define BLOCK_SIZE 8388608

namespace eclipse {
//const int BLOCK_SIZE = (const int)context.settings.get<int>("filesystem.block");
const int default_shm_id = 85549;

struct shm_info {
//	char *buf; //[BLOCK_SIZE];
	char buf[BLOCK_SIZE];
	uint64_t chunk_size;
	uint32_t chunk_index;
	bool commit;
};
	
class lblock_manager {
public :
	
	//std::shared_ptr<std::mutex> lock;
	std::shared_ptr<std::ifstream> fd;
//	std::map<uint32_t, std::vector<char> > pre_buf;
//	std::vector<struct shm_info*> shm_idx;	
	uint32_t chunk_num;
	uint32_t processed_chunk_num;
	uint32_t to_process_chunk_num;
	uint32_t cur_max_cidx;
	
	//shm_info* shared_memory;
	void* shared_memory;
	char** pre_buf;
	int shmid;
	int* end;


	uint64_t index_addr;
	bool* check_addr;
	uint64_t buf_addr;

	int count;
	std::vector<eclipse::messages::BlockInfo> assigned_chunks;

	lblock_manager(uint32_t cn, std::shared_ptr<std::ifstream> _fd, std::shared_ptr<std::mutex> l, std::vector<eclipse::messages::BlockInfo>& ac) 
		: count(0), chunk_num(cn), processed_chunk_num(0), to_process_chunk_num(0), fd(_fd), assigned_chunks(ac) {

		/*count = 0;
		chunk_num = cn;
		processed_chunk_num = 0;	
		to_process_chunk_num = 0;
		fd = _fd;
		lock = l;
		assigned_chunks = ac;*/
	}
	~lblock_manager(){

	}
};

using vec_str = std::vector<std::string>;

class BlockNode: public Node {
  public:
    BlockNode (network::ClientHandler*);
    ~BlockNode ();

    //! @brief Save to disk a block and replicate.
    bool block_insert_local(Block&, bool replicate = true);

    //! @brief Delete Local block
    bool block_delete_local(Block&, bool replicate = true);

    //! @brief Update the content of the block.
    //bool block_update_local(Block& block, uint32_t pos, uint32_t len, bool replicate = true);
    bool block_update_local(Block& block, uint64_t pos, uint64_t len, bool replicate = true);

    //! @brief Read block from the local node.
    bool block_read_local(Block& block, uint64_t off = 0, uint64_t len = 0, bool ignore_params = true);

	void lblock_manager_init(string file, std::vector<eclipse::messages::BlockInfo>& assigned_chunks, int lbm_id);
    void lbm_read(int lid);
	void lblock_manager_stop(int lid);
	bool lblock_manager_destroy(int lid);
  protected:
    void replicate_message(messages::IOoperation*);

    Local_io local_io;
    int network_size;

	int task_num;
	//vector<struct lblock_manager*> lblock_managers;
	map<int, struct lblock_manager*> lblock_managers;
//	int task_num;
};

}
