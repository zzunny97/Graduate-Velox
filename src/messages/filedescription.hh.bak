#pragma once

#include "fileinfo.hh"
#include "../common/logical_block_metadata.hh"
#include <vector>
#include <map>

namespace eclipse {
namespace messages {

struct FileDescription: public FileInfo {
  FileDescription() = default;
  ~FileDescription() = default;

  FileDescription& operator=(FileDescription&);

  std::string get_type() const override;

  std::vector<std::string> HB_blocks;
  std::vector<std::string> blocks;
  std::vector<uint32_t> hash_keys;

  std::vector<uint64_t> block_size;
  std::vector<uint64_t> offsets;
  std::vector<uint64_t> offsets_in_file;
  std::vector<uint32_t> small_block_sequences;
  std::vector<uint32_t> huge_block_sequences;
	std::vector<std::string> block_hosts;
	
	std::vector<logical_block_metadata> logical_blocks; // TODO, refactor this hell

  uint32_t num_static_blocks = 0;
};

}
}

