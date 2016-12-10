package tinySQL;

import java.util.ArrayList;
import storageManager.Block;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;
public class RelationHelper {
	
	public static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
		Block block_reference;
	    if (relation_reference.getNumOfBlocks()==0) {
	      block_reference=mem.getBlock(memory_block_index);
	      block_reference.clear(); //clear the block
	      block_reference.appendTuple(tuple); // append the tuple
	      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
	    } else {
	      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
	      block_reference=mem.getBlock(memory_block_index);

	      if (block_reference.isFull()) {
	        block_reference.clear(); //clear the block
	        block_reference.appendTuple(tuple); // append the tuple
	        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
	      } else {
	        block_reference.appendTuple(tuple); // append the tuple
	        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
	      }
	    }
	}
	//append the blocks in main memory to the relation
	public static void appendMemToRelation(Relation relation_reference, MainMemory mem, ArrayList<Tuple> tuples) {
		Block block_reference;
		//clear the memory
	  clearMainMemory(mem);
	  int mem_block_index = 0;
	  for (int i = 0; i < tuples.size(); i++) {
		  Tuple tuple = tuples.get(i);
		  if (mem.getBlock(mem_block_index).isFull())
		  	mem_block_index += 1;
		  block_reference = mem.getBlock(mem_block_index);
	      block_reference.appendTuple(tuple); // append the tuple
	      if ((i+1) == tuples.size() || mem_block_index + 1== mem.getMemorySize())
	      {
	    	  if (relation_reference.getNumOfBlocks() == 0) 
	    		  relation_reference.setBlocks(0, 0, mem_block_index+1);
	    	  else 
	    		  relation_reference.setBlocks(relation_reference.getNumOfBlocks(), 0, mem_block_index+1);
	    	  mem_block_index = 0;
	    	  clearMainMemory(mem);
	      }
	  }
	}
	
	public static void clearMainMemory(MainMemory mem) {
		for (int i = 0; i < mem.getMemorySize(); i++) 
			 mem.getBlock(i).clear();
	}
}
