package tinySQL;

import java.util.ArrayList;
import storageManager.*;
public class SingleTableScan {
	private SchemaManager schema_manager;
	private MainMemory mem;
	
	public SingleTableScan(SchemaManager schema_manager, MainMemory memory) {
		this.schema_manager = schema_manager;
		this.mem = memory;
	}
	
	public  ArrayList<Tuple> tableScan(String table, TreeNode selecdeletetree, int action) {
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		Relation relation_reference = schema_manager.getRelation(table);
		int memnumBlocks = mem.getMemorySize();
		ArrayList<Tuple> tuples = new ArrayList<Tuple> ();

		//retrieve blocks in two pass because the memory size is only 10 blocks
		int relationnumBlocks = relation_reference.getNumOfBlocks();
		int alreadyreadblocks = 0;
		while (relationnumBlocks > 0) {
			int senttomem = memnumBlocks > relationnumBlocks?relationnumBlocks:memnumBlocks;
			relation_reference.getBlocks(alreadyreadblocks,0,senttomem);
			if (action == 0)
				tuples.addAll(selectTuples(relation_reference, senttomem, selecdeletetree));
			else if (action == 1)
				deleteTuples(relation_reference, senttomem, selecdeletetree, alreadyreadblocks);
		    relationnumBlocks -= senttomem;
		    alreadyreadblocks += senttomem;
		}
		return tuples;
	}
	
	private ArrayList<Tuple> selectTuples(Relation relation_reference, int senttomem, TreeNode select)
	{
		ExpressionTree tree = select.conditions;
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	    for (int i = 0; i < senttomem; i++) {
	    	Block block_reference=mem.getBlock(i);
	    	//this is to handle the holes after deletion
	    	if (block_reference.getNumTuples() == 0) continue;
	    	for (Tuple tup: block_reference.getTuples()) {
	    		if (tree == null || (tree != null && tree.check(relation_reference.getSchema(), tup)))
	    				tuples.add(tup);
	    	}
	    }
	    return tuples;
	}
	
	private void deleteTuples(Relation relation_reference, int senttomem, TreeNode delete, int alreadyreadblocks) {
		ExpressionTree tree =delete.conditions;
	    for (int i = 0; i < senttomem; i++) {
	    	Block block_reference=mem.getBlock(i);
	    	//this is to handle the holes after deletion
	    	if (block_reference.getNumTuples() == 0)
	    		continue;
	    	ArrayList<Tuple> tuples = block_reference.getTuples();
	    	for (int j = 0; j < tuples.size(); j++) {
	    		if (tree != null)
	    		{ 
	    			if (tree.check(relation_reference.getSchema(), tuples.get(j)))
	    			block_reference.invalidateTuple(j);
	    		}
	    		else 
	    			block_reference.invalidateTuples();
	    	}
	    }
	    relation_reference.setBlocks(alreadyreadblocks, 0, senttomem);
	}
}
