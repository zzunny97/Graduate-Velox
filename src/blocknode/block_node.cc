// includes & usings {{{
#include "block_node.hh"

// zzunny add
#include <cstdio>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
// zzunny end

using namespace eclipse;
using namespace eclipse::messages;
using namespace eclipse::network;
using namespace std;

// }}}


namespace eclipse {
// Constructor & destructor {{{
//int BLOCK_SIZE = context.settings.get<int>("filesystem.block");
int SLOT = context.settings.get<int>("addons.cores");
int PRE_READ_BUF = 5;

BlockNode::BlockNode (ClientHandler* net) : Node () { 
  network = net;
  network_size = context.settings.get<vec_str>("network.nodes").size();
}

BlockNode::~BlockNode() { }
// }}}
// replicate_message {{{
//! @brief Compute the right and left node of the current node
//! and send its replicas of the given block
void BlockNode::replicate_message(IOoperation* m) {
  vector<int> nodes;
  for (int i=1; i < 3; i++) {
    if(i%2 == 1) {
      nodes.push_back ((id + (i+1)/2 + network_size) % network_size);
    } else {
      nodes.push_back ((id - i/2 + network_size) % network_size);
    }
  }
  network->send_and_replicate(nodes, m);
}
// }}}
// block_insert_local {{{
//! @brief This method insert the block locally and replicated it.
bool BlockNode::block_insert_local(Block& block, bool replicate) {
  local_io.write(block.first, block.second);
  INFO("[DFS] BLOCK: %s SIZE: %lu", block.first.c_str(), block.second.length());
//  cout << "[DFS] BLOCK: " << block.first.c_str()<<" SIZE: " << block.second.length() << " OFF : " << block;

  if (replicate) {
    INFO("[DFS] Saving locally BLOCK: %s", block.first.c_str());
    IOoperation io;
    io.operation = messages::IOoperation::OpType::BLOCK_INSERT_REPLICA;
    io.block = move(block);
    replicate_message(&io);
  } else {
    INFO("[DFS] Saving replica locally BLOCK: %s", block.first.c_str());
  }

  return true;
}
// }}}
// block_read_local {{{
//! @brief This method read the block locally.
bool BlockNode::block_read_local(Block& block, uint64_t off, uint64_t len, bool ignore_params) {
  INFO("BLOCK REQUEST: %s [%lu,%lu]", block.first.c_str(), off, len);
  block.second = local_io.read(block.first, off, len, ignore_params);
  return true;
}

// }}}
// block_delete_local {{{
//! @brief This method read the block locally.
bool BlockNode::block_delete_local(Block& block, bool replicate) {
  local_io.remove(block.first);

  INFO("[DFS] Removed locally BLOCK: %s", block.first.c_str());

  if (replicate) {
    IOoperation io;
    io.operation = messages::IOoperation::OpType::BLOCK_DELETE_REPLICA;
    io.block = move(block);
    replicate_message(&io);
  }

  return true;
}
// }}}
// block_update_local {{{
//bool BlockNode::block_update_local(Block& block, uint32_t pos, uint32_t len, bool replicate) {
bool BlockNode::block_update_local(Block& block, uint64_t pos, uint64_t len, bool replicate) {
  local_io.update(block.first, block.second, pos, len);

  if (replicate) {
    INFO("Block %s updated real host", block.first.c_str());
    IOoperation io;
    io.operation = messages::IOoperation::OpType::BLOCK_UPDATE_REPLICA;
    io.pos = pos;
    io.length = len;
    io.block = move(block);
    replicate_message(&io);

  } else {
    INFO("Block replica %s updated real host", block.first.c_str());
  }
  return true;
}


void BlockNode::lbm_read(int lid) {
	auto& LBM = lblock_managers[lid];
  	int total_chunks_num = LBM->assigned_chunks.size(); 
	int total = PRE_READ_BUF * SLOT;
	int* begin = (int*)(LBM->index_addr + total * sizeof(shm_info));
	LBM->end = (int*)((uint64_t)begin + sizeof(int));

	int i = 0;
	int processed = 0;
	int check_point = 0, write_num, consumed_num;
	int pre_read_num = PRE_READ_BUF * SLOT;
	while( processed < total_chunks_num){
		//cur_buf = LBM->shared_memory[i%pre_read_num];
		//cout << *(LBM->end) << endl;
		//if(*(LBM->end) == SLOT) {
	//	if(LBM->end_client == SLOT) {
			//cout << "processed = " << processed << " total_chunks_num = " << total_chunks_num << endl;
	//		cout << "End bit reached " << SLOT << "end reading" << endl;
	//		break;
	//	}
		if( i % pre_read_num == 0){
			int end_client_num = 0;
			for( int i = 0; i < SLOT; i++){
				if((LBM->check_addr[i])){
					end_client_num++;
				}
			}

			if(end_client_num == SLOT)
				break;
		}

		shm_info* cur_index = (shm_info*)(LBM->index_addr + sizeof(shm_info) * (i%pre_read_num));
		if(!cur_index->commit) {
			//cout << "Fill the Buf with Read Chunk : " << (uint64_t)cur_index << endl;
			LBM->fd->seekg(LBM->assigned_chunks[processed].offset, ios::beg);
			//cout << "Move Seek : " << (uint64_t)(cur_index->buf) << " : " << cur_index->buf <<  endl;
			//cout << "Move Seek : " << (uint64_t)(cur_index) << " : " << cur_index->buf <<  endl;
			LBM->fd->read(cur_index->buf, LBM->assigned_chunks[processed].size);
			cur_index->chunk_size = LBM->assigned_chunks[processed].size;
			cur_index->chunk_index = LBM->assigned_chunks[processed].seq;
			cur_index->commit = true;
			processed++;
			cout << "Chunk Index : " << cur_index->chunk_index << " Stacked Buf Num : " << processed << endl;
		}
		
		i = i+1 < total_chunks_num ? i+1 : 0;
	}
	
	*(LBM->end) = 1000;
	cout << "End bit become 1000" << endl;
}


void BlockNode::lblock_manager_init(string file, std::vector<eclipse::messages::BlockInfo>& assigned_chunks, int lbm_id){
  	string disk_path = context.settings.get<string>("path.scratch");
	std::shared_ptr<std::mutex> lock(new std::mutex);
	std::shared_ptr<ifstream> in(new ifstream);
	in->open(disk_path + string("/") + assigned_chunks[0].HBname, ios::in | ios::binary);
	if(!in->good()){
		cout << "File " << file << " Open Fail" << endl;
		return;
	}
	
	cout << "Make LBM Manger : " << assigned_chunks[0].HBname << endl;
	struct lblock_manager *lbm = new struct lblock_manager(PRE_READ_BUF, in, lock, assigned_chunks);
	//lblock_managers.push_back(lbm);
	lblock_managers.insert({lbm_id, lbm});
		
	//int cur_task_num = task_num;
	auto& LBM = lblock_managers[lbm_id];
	uint64_t total_buf_size = sizeof(shm_info) * PRE_READ_BUF * SLOT + sizeof(int)*2 + sizeof(bool)*SLOT;
	cout << "LBM ID : " << lbm_id << endl;
	LBM->shmid = shmget((key_t)(lbm_id + default_shm_id), total_buf_size, 0666|IPC_CREAT);
	if(LBM->shmid == -1){
		perror("shmget failed");
		return; 
	}

	LBM->shared_memory = shmat(LBM->shmid, (void*)0, 0);
	if(LBM->shared_memory == (void*)-1){
		perror("shmat failed");
		return;
	}

	memset((void*)LBM->shared_memory, 0, total_buf_size);
	LBM->index_addr = (uint64_t)LBM->shared_memory;
	LBM->check_addr = (bool*)((uint64_t)LBM->shared_memory + sizeof(shm_info)*SLOT*PRE_READ_BUF + sizeof(int)*2);
	if(LBM->shared_memory == (void*)-1){
		perror("shmat failed");
		return;
	}
	
	//task_num++;
	//return cur_task_num;	
	cout << "Init end, start lbm_read" << endl;
	struct timeval start, end;
	double diffTime = 0;
	gettimeofday(&start, NULL);
	lbm_read(lbm_id);
	gettimeofday(&end, NULL);
	diffTime = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec)/1000000.0;
	cout << "Read time = " << (double)diffTime << endl;
	cout << "LBM read done" << endl;
}

void BlockNode::lblock_manager_stop(int lid){
	auto& LBM = lblock_managers[lid];
	cout<< "Get Stop Message From Client"<<endl;
//	(*(LBM->end))++;
//	LBM->end_client++;
	cout<< "Add a End Count"<<endl;
}
bool BlockNode::lblock_manager_destroy(int lid){
	cout << "Destroy Lblock Manager : " << lid << endl;
	delete[] lblock_managers[lid];
}

}
