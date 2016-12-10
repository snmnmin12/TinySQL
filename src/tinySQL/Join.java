package tinySQL;
import java.util.ArrayList;

import storageManager.*;
/*
 * @author: Mingmin Song
 */
public class Join {
	PhiQuery phi;
	private SchemaManager schema_manager;
	private MainMemory mem;
	ExpressionTree tree;
	Join(PhiQuery phi, ExpressionTree tree) {
		this.phi = phi;
		this.schema_manager = phi.schema_manager;
		this.mem = phi.mem;
		this.tree = tree;
	}
	
	//here to joine 2 tables with nested loop 
	//sort tables according to the tables's tuple numbers
	void sortable(ArrayList<String> tables) {
		assert(tables.size() == 2);
		ArrayList<String> temptable = new ArrayList<String>();
		String table1 = tables.get(0);
		String table2 = tables.get(1);
		if (schema_manager.getRelation(table1).getNumOfBlocks() > 
		schema_manager.getRelation(table2).getNumOfBlocks()) {
			temptable.add(table2);
			temptable.add(table1);
		}
		else {
			temptable.add(table1);
			temptable.add(table2);
		}
		tables = temptable;
	}

	//perfrom natural join after the the first pass for sublist
	private ArrayList<UTuple> NaturalJoinSubList(ArrayList<String> tables, ArrayList<ArrayList<Integer>> records, ArrayList<String> fieldkeys)
	{
		RelationHelper.clearMainMemory(mem);
		ArrayList<UTuple> utuples = new ArrayList<UTuple>();
		Heap<UTuple> heap1 = new Heap<UTuple>();
		Heap<UTuple> heap2 = new Heap<UTuple>();
		//here the size should be four
		assert(tables.size() == 4);
		ArrayList<Integer> tablesublist1 = records.get(0);
		ArrayList<Integer> tablesublist2 = records.get(1);
		ArrayList<Integer> offset1 = new ArrayList<Integer>();
		ArrayList<Integer> offset2 = new ArrayList<Integer>();
		Relation relation_reference1 = schema_manager.getRelation(tables.get(2));
		Relation relation_reference2 = schema_manager.getRelation(tables.get(3));
		
		int numsublist1 = tablesublist1.size();
		int numsublist2 = tablesublist2.size();
		
		//initialize the two sublist first
		offset1.add(0);
		for (int i = 1; i < tablesublist1.size(); i++)
			offset1.add(tablesublist1.get(i-1));
		
		offset2.add(0);
		for (int i = 1; i < tablesublist2.size(); i++)
			offset2.add(tablesublist2.get(i-1));
		
		//read into main memory
		TwoPass.checkMemory(mem,0, numsublist1, offset1, tablesublist1, relation_reference1,fieldkeys.get(0), heap1);
		TwoPass.checkMemory(mem,numsublist1, numsublist2, offset2, tablesublist2, relation_reference2,fieldkeys.get(1), heap2);
		
		//processing heap element in the main memory
		ArrayList<UTuple> utuples1 = new ArrayList<UTuple>();
		ArrayList<UTuple> utuples2 = new ArrayList<UTuple>();
		//while(sum(offset1) != totalnum1 && sum(offset2) != totalnum2) {
		while(!heap1.isEmpty() && !heap2.isEmpty()) {
			UTuple ut1 = heap1.peek();
			UTuple ut2 = heap2.peek();
			if (utuples1.size() != 0 && ut1.compareTo(utuples1.get(0)) != 0) 
				JointTwoUTuple(utuples1, utuples2, utuples);
			
			boolean flag = false;
			//if this is true, continue to collect all the tuples
			if (utuples1.size() != 0 && ut1.compareTo(utuples1.get(0)) == 0) {
				TwoPass.RemoveCollectTuples(mem, heap1, utuples1);
				flag = true;
			}
			//if this is true, continue to collect all the tuples	
			if (utuples2.size() != 0 && ut2.compareTo(utuples2.get(0)) == 0) {
				TwoPass.RemoveCollectTuples(mem, heap2, utuples2);
				flag = true;
			}
			
			if (!flag) {			
					int cmp = ut1.compareTo(ut2);
					if (cmp == 0) {		
						//collect all similar tuples in main memeory
						while(!heap1.isEmpty() && heap1.peek().compareTo(ut1) == 0) {
							TwoPass.RemoveCollectTuples(mem, heap1, utuples1);
						}	
						//collect all similar tuples in main memeory
						while(!heap2.isEmpty() && heap2.peek().compareTo(ut2) == 0) {
							TwoPass.RemoveCollectTuples(mem,heap2, utuples2);
						}
					}else if (cmp < 0){
						UTuple u1 = heap1.remove();
						TwoPass.InvalidateTuples(mem, u1.blockindex,u1.tupleindex);
					}else {
						UTuple u2 = heap2.remove();
						TwoPass.InvalidateTuples(mem,  u2.blockindex, u2.tupleindex);
					}
			}
			TwoPass.checkMemory(mem,0, numsublist1, offset1, tablesublist1, relation_reference1,fieldkeys.get(0), heap1);
			TwoPass.checkMemory(mem, numsublist1, numsublist2, offset2, tablesublist2, relation_reference2,fieldkeys.get(1), heap2);
		}
		//join remaining tuples
		JointTwoUTuple(utuples1, utuples2, utuples);
		return utuples;
	}
	//to join two utuples into one utuple
	private void JointTwoUTuple(ArrayList<UTuple> utuples1, ArrayList<UTuple> utuples2, ArrayList<UTuple> utuples)
	{	
		if (utuples1.size() != 0 && utuples2.size() != 0) {
			for (UTuple t1: utuples1)
				for (UTuple t2: utuples2)
					utuples.add(t1.JoinUTuple(t2));
			utuples1.clear();
			utuples2.clear();
		}
	}

	//to clear main memory
	
	//perform natural join for 2 tables
	public ArrayList<Tuple> NaturalJoin2Tables(ArrayList<String> tables,ArrayList<String> fieldkeys) {
		ArrayList<UTuple> tuples = new ArrayList<UTuple>();
		//two pass to sort tables first
		ArrayList<ArrayList<Integer>> records = new ArrayList<ArrayList<Integer>>();
		int originaltablesize = tables.size();
		String temp = "temp";
		ArrayList<String> fieldnames = new ArrayList<String>();
		
		for (int i = 0; i < originaltablesize; i++) {
			String relationname = tables.get(i);
			String relationnametemp = relationname+temp;
			tables.add(relationname+temp);
			Relation relation_reference = schema_manager.getRelation(relationname);
			operation(relation_reference,fieldnames, relationname);
			Schema schema = new Schema(relation_reference.getSchema());
			Relation relation_referencetemp = schema_manager.createRelation(relationnametemp, schema);	
			//String attri = fieldkeys.get(i).substring(fieldkeys.get(i).indexOf('.')+1);
			ArrayList<Integer> record = TwoPass.FirstPass(relation_reference, mem, relation_referencetemp, fieldkeys.get(i));
			records.add(record);
		}
		tuples = NaturalJoinSubList(tables, records,fieldkeys);
		ArrayList<Tuple> temptuples= toTuples(tables,tuples, fieldnames);
		ArrayList<Tuple> output = new ArrayList<Tuple>();
		for (Tuple tup:temptuples) {
			if (tree == null ||(tree != null && tree.check(tup.getSchema(), tup)))  
				output.add(tup);
		}
		return output;
	}
	//totuples here
	private ArrayList<Tuple> toTuples(ArrayList<String> tables, ArrayList<UTuple> output, ArrayList<String> attributes) {
		String tablename = "totuples";
		tables.add(tablename);
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		for (int i = 0; i < output.get(0).fields().size(); i++) 
			field_types.add(output.get(0).fields().get(i).type);
		Schema schema=new Schema(attributes,field_types);
		Relation relation_reference = schema_manager.createRelation(tablename, schema);
		ArrayList<Tuple> tuples = UTuple.UtoTuples(relation_reference, output);
		return tuples;
	}

	//pefrome natural join for 3 tables
	public ArrayList<UTuple> NaturalJoin3Tables(ArrayList<String> tables) {
		ArrayList<UTuple> tuples = new ArrayList<UTuple>();
		return tuples;
	}
	
	/***********finished the natural join for 2 tables**********************************
	 * ***********************************************************/
	public ArrayList<Tuple> CrossJoin2Tables(ArrayList<String> tables) {
		/*This is to join 2 tables together
		 */
		assert(tables.size() == 2);
		sortable(tables);
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		String table1 = tables.get(0);
		String table2 = tables.get(1);
		
		Relation rela_reference1 = schema_manager.getRelation(table1);
		Relation rela_reference2 = schema_manager.getRelation(table2);
		
		//we have to create fields
		ArrayList<String> fieldnames = new ArrayList<String>();
		operation(rela_reference1,fieldnames, table1);
		operation(rela_reference2,fieldnames, table2);
		
		//create new schemas
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>(rela_reference1.getSchema().getFieldTypes());
		fieldtypes.addAll(rela_reference2.getSchema().getFieldTypes());
		
		//create new intermediate relation
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = table1+table2;
		tables.add(relationname);
		Relation relation_reference = schema_manager.createRelation(relationname,schema);		
		//read on block from relation 1 and remaining memory blocks for relation2
		int relationnumBlocks1 = rela_reference1.getNumOfBlocks();
		int relationnumBlocks2 = rela_reference2.getNumOfBlocks();
		int memnumBlocks = mem.getMemorySize();
		
		//if the size of table 1 less than mainmemory then read all inside
		int step = relationnumBlocks1<memnumBlocks?relationnumBlocks1:memnumBlocks/2+1;
		int senttomem1 = step;
		int senttomem2 = memnumBlocks - step;		
		
		for (int i = 0; i < relationnumBlocks1; i +=senttomem1) {
			
			ArrayList<Integer> inmemrecord = new ArrayList<Integer>();
			if (i+senttomem1 > relationnumBlocks1)
				senttomem1 = relationnumBlocks1 - i;
			rela_reference1.getBlocks(i,0,senttomem1);
			inmemrecord.add(senttomem1);
			
			//read 1/2 memory size blocks from relation 2 to main memory
			for (int j = 0; j < relationnumBlocks2; j+= senttomem2) {
				if (j+senttomem2 > relationnumBlocks2)
					senttomem2 = relationnumBlocks2 - j;
				rela_reference2.getBlocks(j,senttomem1,senttomem2);
				if (inmemrecord.size() == 2)
					inmemrecord.remove(inmemrecord.size()-1);
				inmemrecord.add(senttomem1+senttomem2);
				
				//read the remaining from relation 3 to main memory
				Tuple tuple = relation_reference.createTuple();
				ProcessingMem(relation_reference,
						inmemrecord, 0, 0, tuple, 0,tuples);
			}
		}
		
		return tuples;
	}
	
	//here to join 3 tables with nested loop 
	public ArrayList<Tuple> CrossJoin3Tables(ArrayList<String> tables) {
		/*This is to join 2 tables together
		 */
		assert(tables.size() == 3);
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		String table1 = tables.get(0);
		String table2 = tables.get(1);
		String table3 = tables.get(2);
		
		Relation rela_reference1 = schema_manager.getRelation(table1);
		Relation rela_reference2 = schema_manager.getRelation(table2);
		Relation rela_reference3 = schema_manager.getRelation(table3);
		
		//we have to create fields
		ArrayList<String> fieldnames = new ArrayList<String>();
		operation(rela_reference1,fieldnames, table1);
		operation(rela_reference2,fieldnames, table2);
		operation(rela_reference3,fieldnames, table3);
		
		//create new schemas
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>(rela_reference1.getSchema().getFieldTypes());
		fieldtypes.addAll(rela_reference2.getSchema().getFieldTypes());
		fieldtypes.addAll(rela_reference3.getSchema().getFieldTypes());
		
		//create new intermediate relation
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = table1+table2+table3;
		tables.add(relationname);
		Relation relation_reference = schema_manager.createRelation(relationname,schema);		
		//read on block from relation 1 and remaining memory blocks for relation2
		int relationnumBlocks1 = rela_reference1.getNumOfBlocks();
		int relationnumBlocks2 = rela_reference2.getNumOfBlocks();
		int relationnumBlocks3 = rela_reference3.getNumOfBlocks();
		int memnumBlocks = mem.getMemorySize();
		
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		int step = memnumBlocks/2+1;
		int senttomem1 = step;
		int senttomem2 = step/2;		
		
		for (int i = 0; i < relationnumBlocks1; i +=senttomem1) {
			//read 1/3 memory blocks of relations 1 to main memory
			ArrayList<Integer> inmemrecord = new ArrayList<Integer>();
			
			if (i+senttomem1 > relationnumBlocks1)
				senttomem1 = relationnumBlocks1 - i;
			rela_reference1.getBlocks(i,0,senttomem1);
			inmemrecord.add(senttomem1);
			
			//read 1/3 memory size blocks from relation 2 to main memory
			for (int j = 0; j < relationnumBlocks2; j+= senttomem2) {
				if (j+senttomem2 > relationnumBlocks2)
					senttomem2 = relationnumBlocks2 - j;
				rela_reference2.getBlocks(j,senttomem1,senttomem2);
				int senttomem3 = memnumBlocks - senttomem1 - senttomem2;
				while(inmemrecord.size() >= 2)
					inmemrecord.remove(inmemrecord.size()-1);
				inmemrecord.add(senttomem1+senttomem2);
				
				//read the remaining from relation 3 to main memory
				for (int k = 0; k < relationnumBlocks3; k += senttomem3) {
					if (k+senttomem3 > relationnumBlocks3)
						senttomem3 = relationnumBlocks3 - k;
					rela_reference3.getBlocks(k,senttomem1+senttomem2,senttomem3);
					if (inmemrecord.size() == 3)
						inmemrecord.remove(2);
					inmemrecord.add(senttomem1+senttomem2+senttomem3);
					
					Tuple tuple = relation_reference.createTuple();
					ProcessingMem(relation_reference,
							inmemrecord, 0, 0, tuple, 0,tuples);
				}
			}
		}
		return tuples;
	}
	
	//let's join as many table as possible
	public ArrayList<Tuple> JoinManyTables(ArrayList<String> tables) {
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
		//create temp schema and relation
		Schema schema=new Schema(fieldnames,fieldtypes);
		Relation relation_reference = schema_manager.createRelation(relationname,schema);
		//add the templ realtion to the table list, and this is will be rmeoved in  the end;
		tables.add(relationname);
		//create tuples for
		Tuple tuple = relation_reference.createTuple();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();//processing hash and heap
		ProcessingMem(relation_reference,inmemrecord, 0, 0, tuple, 0, tuples);
		return tuples;
		//now processing the blocks in memeory
	}
	//this is the helper function for processing many tables
	private void ProcessingMem(Relation relation_reference,
			ArrayList<Integer>records, int start, int k, Tuple tuple, int tstart, ArrayList<Tuple> tuples) {
		if (k == records.size()) {
			if ((tree == null) || (tree != null && tree.check(tuple.getSchema(), tuple))) {
				Tuple temp = relation_reference.createTuple();
				createTuple(tuple,temp,0);
				tuples.add(temp);
			}
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
	
	//this is helper function for operation to build tuple attributes and fields
	private void operation(Relation rela_reference, ArrayList<String> attrs3, String table) {
		for (String attr:  rela_reference.getSchema().getFieldNames()) {
			int index = attr.indexOf(".");
			if (index == -1)
				attrs3.add(table+"."+attr);
			else
				attrs3.add(attr);
		}
	}
	
	/*******************************************************************************/
	//default table join
	public ArrayList<Tuple> defaultTableJoin(ArrayList<String>tables, TreeNode select) {
		ArrayList<Tuple> jointuples = new ArrayList<Tuple>();
		String[] alltables = tables.toArray(new String[tables.size()]);
		String pretable = null;
		String curtable = null;
		for (int i = 0; i < alltables.length; i++) 
		{
			curtable = alltables[i];
			boolean lasttable = (i==alltables.length-1)?true:false;
			if (pretable != null) {
				pretable = JoinTable(pretable, curtable, lasttable, jointuples);
				tables.add(pretable);
			}
			else {
				pretable = curtable;
			}
		}
		jointuples = new SingleTableScan(schema_manager, mem).tableScan(curtable, select, 0);
		return jointuples;
	}
	//return intermediate table name after join
	//acting as default table join if the above tables join can't be completed
	public String JoinTable(String table1, String table2, boolean lasttable, ArrayList<Tuple> tuples) {
		
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
		
		//create new intermediate relation
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = table1+table2;
		Relation relation_reference = schema_manager.createRelation(relationname,schema);		
		//read on block from relation 1 and remaining memory blocks for relation2
		
		int relationnumBlocks2 = rela_reference2.getNumOfBlocks();
		int memnumBlocks = mem.getMemorySize();
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		int step = memnumBlocks/2;
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
		    		int left = memnumBlocks - senttomem2;
		    		int senttomem1 = left > relationnumBlocks1?relationnumBlocks1:left;
		    		rela_reference1.getBlocks(alreadyreadblocks,senttomem2,senttomem1);
			    	for (int k = senttomem2; k < senttomem1+senttomem2; k++) {
			    		Block block_reference1=mem.getBlock(k);
			    		if (block_reference1.getNumTuples() == 0) continue;
			    		//operation on blocks for relation 1 and relation 2
			    		createTuples(block_reference1, block_reference2,relation_reference,treenode, lasttable, tuples);
			    	}
			    	relationnumBlocks1 -= senttomem1;
			    	alreadyreadblocks += senttomem1;
		    	}
			}
		}
		return relationname;
	}
	
	//helper method to collect tuples
	private void createTuples(Block block_reference1, Block block_reference2,Relation relation_reference, ExpressionTree tree, boolean lasttable, ArrayList<Tuple>tuples) {

		for (Tuple tup: block_reference1.getTuples()) {
			for (Tuple tup2: block_reference2.getTuples()) {
				Tuple tuple = relation_reference.createTuple();
				createTuple(tup, tuple, 0);
				createTuple(tup2, tuple, tup.getNumOfFields());
				if (tree == null ||(tree != null && tree.check(tuple.getSchema(), tuple))) {
					if (lasttable)
						tuples.add(tuple);
					else 
						RelationHelper.appendTupleToRelation(relation_reference, mem, mem.getMemorySize()-1, tuple);
				}
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
