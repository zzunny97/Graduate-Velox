#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace eclipse {
struct SmallBlockMetadata{
	uint32_t size;
	uint64_t offset;
	uint64_t offset_in_file;
	uint32_t small_block_seq;

	std::string name;
	std::string HBname;
  unsigned int huge_block_seq;
};

struct BlockMetadata {
  std::string name;
  std::string file_name;
  unsigned int seq;
  uint32_t hash_key;
//  uint32_t size;
  uint64_t size;
  unsigned int type;
  int replica;
  std::string node;
  std::string l_node;
  std::string r_node;
  unsigned int is_committed;

	std::vector<SmallBlockMetadata> small_blocks;
};

}
