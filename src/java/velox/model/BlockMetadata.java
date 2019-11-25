package com.dicl.velox.model;

import java.lang.String;

public class BlockMetadata {
  public String name;
  public String host;
  public int index;
  public String fileName;
  public long size;
  public long numChunks; 

	public String HBname; // added
	public long  offset; // added
  public long  offset_in_file;
	public int  huge_block_seq;


  public BlockMetadata[] chunks = null;
  public BlockMetadata() { }

  public BlockMetadata(BlockMetadata that) {
    this.name = that.name;
    this.host = that.host;
    this.index = that.index;
    this.fileName = that.fileName;
    this.size = that.size;
    this.numChunks = that.numChunks;

		this.HBname = that.HBname;
		this.offset = that.offset;
		this.offset_in_file = that.offset_in_file;
		this.huge_block_seq = that.huge_block_seq;

    if (that.chunks != null) {
        this.chunks = that.chunks.clone();
    }
  }
}
