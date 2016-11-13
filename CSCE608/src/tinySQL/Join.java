package tinySQL;
import java.util.ArrayList;
import java.util.HashSet;

import storageManager.*;

public class Join {
	
	public static ArrayList<Tuple> JoinTable(SchemaManager schema_manager, ArrayList<Tuple> t1,  ArrayList<Tuple> t2, String table2) {
		
		ArrayList<Tuple> res = new ArrayList<Tuple>();
		ArrayList<String> fieldnames = new ArrayList<String>(t1.get(0).getSchema().getFieldNames());
		HashSet<String> fields = new HashSet<String>(t1.get(0).getSchema().getFieldNames());
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>(t1.get(0).getSchema().getFieldTypes());
		for (String str:t2.get(0).getSchema().getFieldNames()) {
			if (fields.contains(str))
				fieldnames.add(table2+"."+str);
			else fieldnames.add(str);	
		}
		
		fieldtypes.addAll(t2.get(0).getSchema().getFieldTypes());
		
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = "temp";
		Relation relation_reference=schema_manager.createRelation(relationname,schema);
		for (Tuple ut1:t1) {
			for (Tuple ut2:t2) {
				Tuple tuple = relation_reference.createTuple();
				createTuple(ut1, tuple, 0);
				createTuple(ut2, tuple, ut1.getNumOfFields());
				res.add(tuple);
			}
		}
		//schema_manager.deleteRelation(relationname);
		return res;
	}
	private static void operation(Relation rela_reference, ArrayList<String> attrs3, String table) {
		for (String attr:  rela_reference.getSchema().getFieldNames()) {
			int index = attr.indexOf(".");
			if (index == -1)
				attrs3.add(table+"."+attr);
			else
				attrs3.add(attr);
		}
	}
	//return table name
	public static String JoinTable2(SchemaManager schema_manager, MainMemory mem, String table1,  String table2) {
		
		Relation rela_reference1 = schema_manager.getRelation(table1);
		Relation rela_reference2 = schema_manager.getRelation(table2);
		
		ArrayList<String> fieldnames = new ArrayList<String>();
		
		ArrayList<FieldType> fieldtypes = new ArrayList<FieldType>(rela_reference1.getSchema().getFieldTypes());
		
		operation(rela_reference1,fieldnames, table1);
		operation(rela_reference2,fieldnames, table2);
		
		fieldtypes.addAll(rela_reference2.getSchema().getFieldTypes());
		
		Schema schema=new Schema(fieldnames,fieldtypes);
		String relationname = table1+table2;
		Relation relation_reference=schema_manager.createRelation(relationname,schema);		
		//read on block from relation 1 and remaining memory blocks for relation2
		int relationnumBlocks2 = rela_reference2.getNumOfBlocks();
		int memnumBlocks = mem.getMemorySize();
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		int senttomem2 = 1;
		for (int i = 0; i < relationnumBlocks2; i +=senttomem2  ) {
			//read one block of relations 1 to memory
			rela_reference2.getBlocks(i,0,senttomem2);
			
			int relationnumBlocks1 = rela_reference1.getNumOfBlocks();
			int alreadyreadblocks = 0;
			
			while (relationnumBlocks1 > 0) {
				int left = memnumBlocks - senttomem2-1;
				int senttomem1 = left > relationnumBlocks1?relationnumBlocks1:left;
				rela_reference1.getBlocks(alreadyreadblocks,senttomem2,senttomem1);
			    for (int j = 0; j < senttomem2; j++) {
			    	Block block_reference2=mem.getBlock(j);
			    	//handle holes
			    	if (block_reference2.getNumTuples() == 0) continue;
			    	//for relation2
			    	for (int k = senttomem2; k < senttomem1+senttomem2; k++) {
			    		Block block_reference1=mem.getBlock(k);
			    		if (block_reference1.getNumTuples() == 0) continue;
			    		createTuples(block_reference2, block_reference1,relation_reference,mem);
			    	}
			    }
			    relationnumBlocks1 -= senttomem1;
			    alreadyreadblocks += senttomem1;
			}
		}
		
		return relationname;
	}
	
	private static void createTuples(Block block_reference1, Block block_reference2,Relation relation_reference, MainMemory mem) {

		for (Tuple tup: block_reference1.getTuples()) {
			for (Tuple tup2: block_reference2.getTuples()) {
				Tuple tuple = relation_reference.createTuple();
				createTuple(tup, tuple, 0);
				createTuple(tup2, tuple, tup.getNumOfFields());
				PhiQuery.appendTupleToRelation(relation_reference, mem, 9, tuple);
//				tuples.add(tuple);
			}
		}
	}
	//helper methods
	private static void createTuple(Tuple tu1, Tuple tuple, int i) {
		for (int j = 0; j < tu1.getNumOfFields(); j++,i++) {
			if (tu1.getField(j).type == FieldType.INT)
				tuple.setField(i, tu1.getField(j).integer);
			else 
				tuple.setField(i, tu1.getField(j).str);
		}
	}
	
}
