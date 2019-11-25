#pragma once
#include "message.hh"
#include <cstdint>

/* Addtion : small_block_seq, offset, file_offest, HB_name */
namespace eclipse {
namespace messages {

  struct BlockInfo: public Message {
	  std::string get_type() const override;

	  std::string name;
	  std::string HBname;
	  std::string file_name;
	  unsigned int seq;
	  uint32_t hash_key;
	  uint64_t size;
	  unsigned int type;
	  int replica;
	  //uint32_t small_block_seq;
	  uint32_t huge_block_seq;
	  uint64_t offset;
	  uint64_t offset_in_file;


	  std::string node;
	  std::string l_node;
	  std::string r_node;
	  unsigned int is_committed;
	  std::string content;
  };
}
}
