package tinySQL;
import java.util.ArrayList;

import storageManager.*;

public class Join {
	private SchemaManager schema_manager;
	private MainMemory mem;
	ExpressionTree tree;
	Join(PhiQuery phi, ExpressionTree tree) {
		this.schema_manager = phi.schema_manager;
		this.mem = phi.mem;
		this.tree = tree;
	}
	//let's join as many table as possible
	public ArrayList<UTuple> JoinManyTables(ArrayList<String> tables) {

		int alreadyinmem = 0;
		ArrayList<Integer> inmemrecord = new ArrayList<Integer>();
		//get all fieldnames and all fieldtypes
		ArrayList<String> fieldnames = new ArrayList<String>();
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>();
		String relationname = "";
		for (int i = 0; i < tables.size(); i++) {
			String table = tables.get(i);
			Relation rela_reference = schema_manager.getRelation(table);
			rela_reference.getBlocks(0,alreadyinmem,rela_reference.getNumOfBlocks());
			alreadyinmem += rela_reference.getNumOfBlocks();
			inmemrecord.add(alreadyinmem);
			operation(rela_reference,fieldnames, table);
			fieldtypes.addAll(rela_reference.getSchema().getFieldTypes());
			relationname += table;
		}
		Schema schema=new Schema(fieldnames,fieldtypes);
		Relation relation_reference = schema_manager.createRelation(relationname,schema);
		Tuple tuple = relation_reference.createTuple();
		ArrayList<UTuple> tuples = new ArrayList<UTuple>();
		ProcessingMem(relation_reference,inmemrecord, 0, 0, tuple, 0, tuples);
		return tuples;
		//now processing the blocks in memeory
	}
	
	private void ProcessingMem(Relation relation_reference,
			ArrayList<Integer>records, int start, int k, Tuple tuple, int tstart, ArrayList<UTuple> tuples) {
		if (k == records.size()) {
			if (tree == null ||(tree != null && tree.check(tuple.getSchema(), tuple)))
				tuples.add(new UTuple(tuple.getField(0),tuple));
			tuple = relation_reference.createTuple();
			return;
		}
		//now start the recursion
		for (int i = start; i < records.get(k); i++) {
			Block block_reference = mem.getBlock(i);
			for (Tuple tup: block_reference.getTuples()) {
				createTuple(tup, tuple, tstart);
				ProcessingMem(relation_reference, records, records.get(k), k+1, tuple, 
						tstart+tup.getNumOfFields(), tuples);
			}
		}
	}
	
	//operation to build tuple attributes and fields
	private void operation(Relation rela_reference, ArrayList<String> attrs3, String table) {
		for (String attr:  rela_reference.getSchema().getFieldNames()) {
			int index = attr.indexOf(".");
			if (index == -1)
				attrs3.add(table+"."+attr);
			else
				attrs3.add(attr);
		}
	}
	//return intermediate table name after join
	public String JoinTable(String table1, String table2, boolean lasttable) {
		
		ArrayList<ExpressionTree> trees = (tree==null)?null:tree.hasSelection();
		
		Relation rela_reference1 = schema_manager.getRelation(table1);
		Relation rela_reference2 = schema_manager.getRelation(table2);
		//select the right expression tree
		int index = tree == null?-1:selectExpressionTree(table1, table2, trees);
		ExpressionTree treenode = index==-1?null:trees.get(index);
		//we have to create fields
		ArrayList<String> fieldnames = new ArrayList<String>();
		operation(rela_reference1,fieldnames, table1);
		operation(rela_reference2,fieldnames, table2);
		
		//create new schemas
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>(rela_reference1.getSchema().getFieldTypes());
		fieldtypes.addAll(rela_reference2.getSchema().getFieldTypes());
		//create new intermedate relation
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = table1+table2;
		Relation relation_reference = schema_manager.createRelation(relationname,schema);		
		//read on block from relation 1 and remaining memory blocks for relation2
		
		int relationnumBlocks2 = rela_reference2.getNumOfBlocks();
		int memnumBlocks = mem.getMemorySize();
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		int step = 8;
		int senttomem2 = step<relationnumBlocks2?step:relationnumBlocks2;
		for (int i = 0; i < relationnumBlocks2; i +=senttomem2  ) {
			//read one block of relations 1 to memory
			if (i+senttomem2 > relationnumBlocks2)
				senttomem2 = relationnumBlocks2 - i;
			rela_reference2.getBlocks(i,0,senttomem2);
			//first retrieve tuples for smaller table
		    for (int j = 0; j < senttomem2; j++) {
		    	Block block_reference2=mem.getBlock(j);
		    	//handle holes
		    	if (block_reference2.getNumTuples() == 0) 
		    		continue;
				int relationnumBlocks1 = rela_reference1.getNumOfBlocks();
				int alreadyreadblocks = 0;
		    	//for relation1
		    	while (relationnumBlocks1 > 0) {
		    		int left = memnumBlocks - senttomem2-1;
		    		int senttomem1 = left > relationnumBlocks1?relationnumBlocks1:left;
		    		rela_reference1.getBlocks(alreadyreadblocks,senttomem2,senttomem1);
			    	for (int k = senttomem2; k < senttomem1+senttomem2; k++) {
			    		Block block_reference1=mem.getBlock(k);
			    		if (block_reference1.getNumTuples() == 0) continue;
			    		//operation on blocks for relation 1 and relation 2
			    		createTuples(block_reference1, block_reference2,relation_reference,treenode, lasttable);
			    	}
			    	relationnumBlocks1 -= senttomem1;
			    	alreadyreadblocks += senttomem1;
		    	}
			}
		}
		return relationname;
	}
	
	//helper method to collect tuples
	private void createTuples(Block block_reference1, Block block_reference2,Relation relation_reference, ExpressionTree tree, boolean lasttable) {

		for (Tuple tup: block_reference1.getTuples()) {
			for (Tuple tup2: block_reference2.getTuples()) {
				Tuple tuple = relation_reference.createTuple();
				createTuple(tup, tuple, 0);
				createTuple(tup2, tuple, tup.getNumOfFields());
				if (tree == null ||(tree != null && tree.check(tuple.getSchema(), tuple))) {
//					if (phi.isOutput() && lasttable)
//						System.out.println(tuple);
//					else 
					PhiQuery.appendTupleToRelation(relation_reference, mem, mem.getMemorySize()-1, tuple);
				}
//				tuples.add(tuple);
			}
		}
	}
	//helper methods
	private  void createTuple(Tuple tu1, Tuple tuple, int i) {
		for (int j = 0; j < tu1.getNumOfFields(); j++,i++) {
			if (tu1.getField(j).type == FieldType.INT)
				tuple.setField(i, tu1.getField(j).integer);
			else 
				tuple.setField(i, tu1.getField(j).str);
		}
	}
	
	//select the right tree
	private int selectExpressionTree(String table1, String table2, ArrayList<ExpressionTree> alltrees) {
		for (int i = 0; i < alltrees.size(); i++) {
			if (alltrees.get(i).toString().contains(table1) && alltrees.get(i).toString().contains(table2))
				return i;
		}
		return 0;
	}
}
