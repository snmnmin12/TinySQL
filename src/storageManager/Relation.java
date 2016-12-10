package storageManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.ListIterator;

/* Each relation is assumed to be stored in consecutive disk blocks 
 * 	on a single track of the disk (in clustered way). 
 * The disk blocks on the track are numbered by 0,1,2,... 
 * You have to copy the disk blocks of the relation to memory blocks 
 * 	before accessing the tuples 
 * inside the blocks.
 * To delete tuples in the disk blocks, you can invalidate the tuples 
 * 	and left "holes" in the original places. 
 * 	Be sure to deal with the holes when doing every SQL operation.
 * You can decide whether to remove trailing "holes" in a relation.
 * Usage: Create an empty relation through SchemaManager class, 
 * 			not through here.
 *        Create a tuple of the relation through here.
 *        Basically you cannot access data or disk blocks through here 
 *        	directly.
 *        Use the Relation class to copy blocks from the disk 
 *        	to the memory or the other direction.
 *        The Relation class will handle the disk part.
 *        You are able to get schema of a particular relation 
 *        	through here.
 */

public class Relation implements Serializable {
	  private SchemaManager schema_manager;
	  private Disk disk;
	  private int schema_index; // points to the schema of the relation
	  private MainMemory mem; // a pointer to the main memory
	  private String relation_name; // name of the relation

	// For internal use only: DO NOT use constructors here. 
	  // Create a relation through schema manager
	  protected Relation() {
		    this.schema_manager=null;
		    this.schema_index=-1;
		    this.relation_name="";
		    this.mem=null;
		    this.disk=null;
	  }

	  protected Relation(SchemaManager schema_manager, int schema_index, 
			  			String relation_name,
	                    MainMemory mem, Disk disk) {
	    this.schema_manager=schema_manager;
	    this.schema_index=schema_index;
	    this.relation_name=relation_name;
	    this.mem=mem;
	    this.disk=disk;
	  }

	  protected void invalidate() {
	    this.schema_manager=null;
	    this.schema_index=-1;
	    this.relation_name="";
	    this.mem=null;
	  }

	  public String getRelationName()  {
	    return relation_name;
	  }

	  // returns the schema of the tuple
	  public Schema getSchema()  {
	    return new Schema(schema_manager.schemas[schema_index]);
	  }

	  //NOTE: Because the operation should not have disk latency,
	  //      it is implemented in Relation instead of in Disk
	  public int getNumOfBlocks()  {
	    ArrayList<Block> data=disk.tracks.get(schema_index);
	    return data.size();
	  }

	  // returns actual number of tuples in the relation
	  //NOTE: Because the operation should not have disk latency,
	  //      it is implemented in Relation instead of in Disk
	  public int getNumOfTuples()  {
	    ArrayList<Block> data=disk.tracks.get(schema_index);
	    int total_tuples=0;
	    for (ListIterator<Block> vit=data.listIterator();vit.hasNext();) {
	      total_tuples+=vit.next().getNumTuples();
	    }
	    return total_tuples;
	  }

	  public boolean isNull()  {
	    return (schema_manager==null || schema_index==-1 || mem==null);
	  }

	//creates an empty tuple of the schema
	  public Tuple createTuple()  {
	    return new Tuple(schema_manager,schema_index);
	  }

	//NOTE: Every call to each of the following 4 functions has 
	  // a simulated disk delay
	  
	    //reads one block from the relation (the disk) and 
	  // stores in the memory
    //returns false if the index is out of bound	  
	  public boolean getBlock(int relation_block_index, 
			  					int memory_block_index)  {
	    if (memory_block_index<0 || 
	    	memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("getBlock ERROR: block index " 
	    		  + memory_block_index + " out of bound in memory" + "\n");
	      return false;
	    }
	    Block b = disk.getBlock(schema_index,relation_block_index);
	    if (!b.isEmpty()) {
	      mem.setBlock(memory_block_index,b);
	      return true;
	    }
	    return false;
	  }

    //reads several blocks from the relation (the disk) and 
	  // stores in the memory
    //returns false if the index is out of bound	  
	  public boolean getBlocks(int relation_block_index, 
			  				int memory_block_index, int num_blocks)  {
	    if (num_blocks<=0) {
	      System.err.print("getBlocks ERROR: num of blocks " 
	    		  + num_blocks + " too few" + "\n");
	      return false;
	    }
	    if (memory_block_index<0 || memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("getBlocks ERROR: block index " 
	    		  + memory_block_index + " out of bound in memory" + "\n");
	      return false;
	    }
	    int i;
	    if ((i=memory_block_index+num_blocks-1)>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("getBlocks ERROR: access to block " +
	      		"out of memory bound: " + i + "\n");
	      return false;
	    }
	    ArrayList<Block> v=disk.getBlocks(schema_index,
	    							relation_block_index,num_blocks);
	    mem.setBlocks(memory_block_index,v);
	    return true;
	  }

    //reads several blocks from the memory and stores on the disk
    // returns false if the index is out of bound	  
	  public boolean setBlock(int relation_block_index, 
			  				int memory_block_index) {
	    if (memory_block_index<0 || 
	    		memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlock ERROR: block index" 
	    		  + memory_block_index + " out of bound in memory" 
	    		  + "\n");
	      return false;
	    }
	    if (relation_block_index<0) {
	      System.err.print("setBlock ERROR: block index " 
	    		  + relation_block_index + " out of bound in relation" 
	    		  + "\n");
	      return false;
	    }
	    // check if the schema is correct
	    ArrayList<Tuple> v = mem.getBlock(memory_block_index).getTuples();
	    Schema s = schema_manager.schemas[schema_index];
	    for (int i=0;i<v.size();i++) {
	      if (!v.get(i).getSchema().equals(s)) {
	        System.err.print("setBlock ERROR: The tuple at offest " 
	        		+ i + " of memory block "
	            + memory_block_index + " has a different schema." + "\n");
	        return false;
	      }
	    }

	    Tuple t=new Tuple(schema_manager,schema_index);
	    t.invalidate(); //invalidates the tuple
	    if (disk.extendTrack(schema_index,relation_block_index+1,t)) {
	      //Actual writing on disk
	      return disk.setBlock(schema_index,relation_block_index,
	    		  mem.getBlock(memory_block_index));
	    }
	    return false;
	  }
	  
    //reads several blocks from the memory and stores on the disk
    // returns false if the index is out of bound
	  public boolean setBlocks(int relation_block_index, 
			  int memory_block_index, int num_blocks) {
	    if (num_blocks<=0) {
	      System.err.print("setBlocks ERROR: num of blocks " 
	    		  + num_blocks + " too few" + "\n");
	      return false;
	    }
	    if (memory_block_index<0 || 
	    		memory_block_index>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlocks ERROR: block index " 
	    		  + memory_block_index + " out of bound in memory" + "\n");
	      return false;
	    }
	    int i;
	    if ((i=memory_block_index+num_blocks-1)>=Config.NUM_OF_BLOCKS_IN_MEMORY) {
	      System.err.print("setBlocks ERROR: access to block " +
	      		"out of memory bound: " + i + "\n");
	      return false;
	    }
	    if (relation_block_index<0) {
	      System.err.print("setBlocks ERROR: block index " 
	    		  + relation_block_index + " out of bound in relation" + "\n");
	      return false;
	    }

	    ArrayList<Block> vb=new ArrayList<Block>();
	    Schema s = schema_manager.schemas[schema_index];
	    int j,k;
	    for (j=memory_block_index;j<memory_block_index+num_blocks;j++) {
	      // check if the schema is correct
	      ArrayList<Tuple> v = mem.getBlock(j).getTuples();
	      for (k=0;k<v.size();k++) {
	        if (!v.get(k).getSchema().equals(s)) {
	          System.err.print("setBlocks ERROR: The tuple at offest " 
	        		  + k + " of memory block "
	          + j + " has a different schema." + "\n");
	          return false;
	        }
	      }
	      vb.add(mem.getBlock(j));
	    }

	    Tuple t=new Tuple(schema_manager,schema_index);
	    t.invalidate(); //invalidates the tuple
	    if (disk.extendTrack(schema_index,relation_block_index+num_blocks,t)) {
	      //Actual writing on disk
	      return disk.setBlocks(schema_index,relation_block_index,vb);
	    }
	    return false;
	  }

	  //delete the block from starting_block_index to the last block
	  // return false if out of bound
	  public boolean deleteBlocks(int starting_block_index) {
	    return disk.shrinkTrack(schema_index,starting_block_index);
	  }

	  public String toString()  {
	    String str="";
	    ArrayList<Block> data=disk.tracks.get(schema_index);
	    int i=0;
	    str+=("******RELATION DUMP BEGIN******" + "\n");
	    str+=schema_manager.schemas[schema_index].fieldNamesToString();
	    str+=("\n");
	    for (ListIterator<Block> vit=data.listIterator();vit.hasNext();) {
	      Block b=vit.next();
	      str+=i + ": ";
	      str+=b.toString();
	      str+=("\n");
	      i++;
	    }
	    str+="******RELATION DUMP END******";
	    return str;
	  }	  
}
