#ifndef __MODEL_BLOCK_METADATA_HH__
#define __MODEL_BLOCK_METADATA_HH__

#include <string>
#include <stdint.h>
#include <vector>

namespace velox {
  namespace model {
    class block_metadata { //logical_block
      public:
      std::string name; //small_block_name
      uint64_t size;
      std::string host;
      int index; // small_block_seq
      int assigned_num; // small_block_seq
      std::string file_name;

			std::string HBname; // added
			uint64_t offset; // added
			uint64_t offset_in_file; // added
			uint32_t small_block_seq; // added
			uint64_t huge_block_seq; // added

      std::vector<block_metadata> chunks; //physical_blocks
    };
  }
}

#endif
