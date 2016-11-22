package tinySQL;

import java.util.ArrayList;

import storageManager.*;

public class Optimization {
	
	public static void joinOptimization(ArrayList<String> tables, SchemaManager schem_manager) {
//		Relation relation_referene = schem_manager.getRelation(tables.get(0));
//		int size = tables.size();
		
	}
	
	public static ArrayList<String> selectOptimization(String[] tables, PhiQuery phi, ExpressionTree tree) {
		if (tables.length == 1 || tree == null) 
				return null;

		ArrayList<ExpressionTree> ets = tree.hasSelection();
		if (ets == null) 
			return null;
		
		ArrayList<String> temptables = new ArrayList<String>();
		for (ExpressionTree node:ets) {
			//check the conditions to be met
			int leftdot = node.getLeft().getOp().indexOf('.');
			int rightdot = node.getRight().getOp().indexOf('.');
			
			if (leftdot != -1 && rightdot != -1 )
				continue;
			
			ExpressionTree attrNode = (leftdot == -1)?node.getRight():node.getLeft();
			String table = attrNode.getOp().substring(0, attrNode.getOp().indexOf('.'));
			temptables.add(createRelation(table, phi, node));
		}
		return temptables;
	}
	
	public static String createRelation(String table, PhiQuery phi, ExpressionTree tree) {
		String relationname = "temp"+table;
		SchemaManager schema_manager = phi.schema_manager;
		MainMemory mem = phi.mem;
		Relation relation_re = schema_manager.getRelation(table);
		int relationnumBlocks = relation_re.getNumOfBlocks();
		int  memnumBlocks = mem.getMemorySize()-1;
		int alreadyreadblocks = 0;
		ArrayList<String> fieldnames=new ArrayList<String>();
		for (String str:relation_re.getSchema().getFieldNames())
			fieldnames.add(table+"."+str);
		Relation relation_reference = schema_manager.createRelation(relationname,new Schema(fieldnames, relation_re.getSchema().getFieldTypes()));	
		do {
			int senttomem = memnumBlocks > relationnumBlocks?relationnumBlocks:memnumBlocks;
			relation_re.getBlocks(alreadyreadblocks,0,senttomem);
		    for (int i = 0; i < senttomem; i++) {
		    	Block block_reference=mem.getBlock(i);
		    	//this is to handle the holes after deletion
		    	if (block_reference.getNumTuples() == 0) continue;
		    	for (Tuple tup: block_reference.getTuples()) {
		    		Tuple tuple = createNewTuple(relation_reference, tup);
		    		if (tree == null || (tree != null && tree.check(relation_reference.getSchema(), tuple)))
		    			PhiQuery.appendTupleToRelation(relation_reference, mem, memnumBlocks, tuple);
		    	}
		    }
		    relationnumBlocks -= senttomem;
		    alreadyreadblocks += senttomem;
		}while (relationnumBlocks > 0);
		return relationname;
	}
	private static Tuple createNewTuple(Relation relation_reference, Tuple tuple) {
		Tuple newtuple = relation_reference.createTuple();
		if (newtuple.getNumOfFields() != tuple.getNumOfFields())
			return null;
		for (int i = 0; i < tuple.getNumOfFields(); i++) {
			Field f = tuple.getField(i);
			if (f.type == FieldType.INT)
				newtuple.setField(i, f.integer);
			else 
				newtuple.setField(i, f.str);
		}
		return newtuple;
	}
	public static void ProjectOptimization()
	{
		
	}

}
