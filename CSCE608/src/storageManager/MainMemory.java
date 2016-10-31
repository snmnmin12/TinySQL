package storageManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.ListIterator;

public class MainMemory implements Serializable {
      // an array of blocks
	  private Block[] blocks=new Block[Config.NUM_OF_BLOCKS_IN_MEMORY]; 

	  public MainMemory() { 
		  for (int i=0;i<Config.NUM_OF_BLOCKS_IN_MEMORY;i++)
			  blocks[i]=new Block();
	  }

	// returns total number of blocks in the memory (including empty ones)
	  public int getMemorySize()  {
	    return Config.NUM_OF_BLOCKS_IN_MEMORY;
	  }

	  // One of the only functions that returns an object reference 
	  // to the data structure
	  // returns null if out of bound
	  public Block getBlock(int memory_block_index) {
	    if (memory_block_index<0 || 
	    		memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("getBlock ERROR: block index " 
	    		  + memory_block_index + " out of memory bound" + "\n");
	      return null;
	    }
	    return blocks[memory_block_index];
	  }

    //can be used to copy a memory block to another memory block
    //returns false if out of bound or tuples do not match the schema	  
	  public boolean setBlock(int memory_block_index,  Block b) {
	    if (memory_block_index<0 || 
	    		memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlock ERROR: block index " 
	    		  + memory_block_index + " out of memory bound" + "\n");
	      return false;
	    }
	    blocks[memory_block_index]=new Block(b);
	    return true;
	  }
	  
	  // For internal use: used by Relation
	  protected boolean setBlocks(int memory_block_index, 
			  					ArrayList<Block> vb) {
	    if (memory_block_index<0 || 
	    		memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlocks ERROR: block index " 
	    		  + memory_block_index + " out of memory bound" + "\n");
	      return false;
	    }
	    if ((memory_block_index+vb.size()-1)
	    		>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlocks ERROR: number of blocks " 
	    		  + vb.size() + " out of memory bound" + "\n");
	      return false;
	    }
	    for (int i=0;i<vb.size();i++) {
	      blocks[memory_block_index+i]=new Block(vb.get(i));
	    }
	    return true;
	  }	  

    //Gets tuples from consecutive blocks from memory
    //   [ memory_block_begin, memory_block_begin+num_blocks-1 ]
    //NOTE: The output tuples must all belong to the same relation/table.
    //IMPORTANT NOTE: Only the valid tuples in the blocks are returned
	  public ArrayList<Tuple> getTuples(int memory_block_begin,
			  							int num_blocks)  { 
	    if (memory_block_begin<0 || 
	    		memory_block_begin>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("getTuples ERROR: block index " 
	    		  + memory_block_begin + " out of memory bound" + "\n");
	      return new ArrayList<Tuple>();
	    }
	    if (num_blocks<=0) {
	      System.err.print("getTuples ERROR: num of blocks "
	    		  + num_blocks + " too few" + "\n");
	      return new ArrayList<Tuple>();
	    }
	    int i;
	    if ((i=memory_block_begin+num_blocks-1)
	    		>=Config.NUM_OF_BLOCKS_IN_MEMORY ) {
	      System.err.print("getTuples ERROR: access to block " +
	      		"out of memory bound: " + i + "\n");
	      return new ArrayList<Tuple>();
	    }
	    ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	    Schema s = 
	    	blocks[memory_block_begin].getTuples().get(0).getSchema();
	    for (i=memory_block_begin;i<memory_block_begin+num_blocks;
	    	i++) {
	      ArrayList<Tuple> tuples2=blocks[i].getTuples();
	      if (!tuples2.get(0).getSchema().equals(s)) {
	        System.err.print("getTuples ERROR: schema at memory " +
	        		"block " + i + " has a different schema" + "\n");
	        return new ArrayList<Tuple>();
	      }
	      // Only valid tuples are returned
	      for (ListIterator<Tuple> it=tuples2.listIterator();
	      		it.hasNext();) {
	        Tuple t=it.next();
	        if (!t.isNull()) tuples.add(new Tuple(t));
	      }
	    }
	    return tuples;
	  }

    //Writes tuples consecutively starting from memory block 
	// memory_block_begin;
    //returns false if out of bound in memory
    //NOTE: The input tuples must all belong to the same relation/table.
	  public boolean setTuples(int memory_block_begin, 
			  				    ArrayList<Tuple> tuples) {
	    if (memory_block_begin<0 || 
	    		memory_block_begin>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setTuples ERROR: block index " 
	    		  + memory_block_begin + " out of memory bound" + "\n");
	      return false;
	    }
	    int tuples_per_block=tuples.get(0).getTuplesPerBlock();
	    int num_blocks=tuples.size()/tuples_per_block;
	    int num_additional_blocks=(tuples.size()%tuples_per_block>0?1:0);
	    if (memory_block_begin + num_blocks + num_additional_blocks >
	        Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setTuples ERROR: number of tuples " +
	      		"exceed the memory space" + "\n");
	      return false;
	    }
	    int t1=0,t2=0;
	    int i;
	    for (i=memory_block_begin;i<memory_block_begin + num_blocks;
	    	i++) {
	      t2+=tuples_per_block;
	      blocks[i].setTuples(tuples,t1,t2);
	      t1=t2;
	    }
	    if (num_additional_blocks==1) {
	      blocks[i].setTuples(tuples,t1,tuples.size());
	    }
	    return true;
	  }

	  public String toString()  {
	    String str="";
	    str+=("******MEMORY DUMP BEGIN******" + "\n");
	    for (int i=0;i<Config.NUM_OF_BLOCKS_IN_MEMORY;i++) {
	      str+=i + ": ";
	      str+=blocks[i].toString();
	      str+=("\n");
	    }
	    str+="******MEMORY DUMP END******";
	    return str;
	  }
}
