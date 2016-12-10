package tinySQL;
import java.util.ArrayList;
import java.util.HashSet;

import storageManager.*;

public class DistinctOperation {
	private SchemaManager schema_manager;
	private MainMemory mem;
	public DistinctOperation(SchemaManager schema_manager, MainMemory mem){
		this.schema_manager = schema_manager;
		this.mem = mem;
	}
	public ArrayList<UTuple> distinct(ArrayList<String> tables, Relation relation_temp, String fieldkey) {
		
		if (relation_temp.getNumOfBlocks() < mem.getMemorySize()){
			return OneTimeMeory(relation_temp, fieldkey);
		}
		
		String temp = "distinct2";
//		String relationname = tables.get(1);
		String relationnametemp = temp;
		tables.add(temp);
//		Relation relation_reference = schema_manager.getRelation(relationname);
		Schema schema = new Schema(relation_temp.getSchema());
		Relation relation_referencetemp = schema_manager.createRelation(relationnametemp, schema);	
		ArrayList<Integer> record = TwoPass.FirstPass(relation_temp, mem, relation_referencetemp, fieldkey);
		ArrayList<UTuple> utuples = distinctOp(tables, relation_referencetemp, record, fieldkey);
		return utuples;
	}
	private ArrayList<UTuple> distinctOp(ArrayList<String> tables, Relation relation_reference, 
			ArrayList<Integer> record, String fieldkey)
	{
		RelationHelper.clearMainMemory(mem);
		ArrayList<UTuple> utuples1 = new ArrayList<UTuple>();
		ArrayList<UTuple> utuples = new ArrayList<UTuple>();
		Heap<UTuple> heap1 = new Heap<UTuple>();
		//here the size should be four
		assert(tables.size() == 3);
		ArrayList<Integer> tablesublist1 = record;
		ArrayList<Integer> offset1 = new ArrayList<Integer>();
		Relation relation_reference1 = relation_reference;
		int numsublist1 = tablesublist1.size();
		//initiaize the two sublist first
		offset1.add(0);
		for (int i = 1; i < tablesublist1.size(); i++)
			offset1.add(tablesublist1.get(i-1));		
		//read into main memory
		TwoPass.checkMemory(mem, 0, numsublist1, offset1, tablesublist1, relation_reference1,fieldkey, heap1);
		//processing heap element in the main memory
		while(!heap1.isEmpty()) {
			UTuple u1 = heap1.peek();
			if (utuples1.size() != 0 && !u1.equals(utuples1.get(0))) 
				outputUTuple(utuples1, utuples);
			boolean flag = false;
			if (utuples1.size() != 0 && u1.equals(utuples1.get(0))) {
				collectUTuple(mem, heap1, utuples1);
				flag = true;
			}
			if (!flag) {
				while(!heap1.isEmpty() && heap1.peek().equals(u1))
				collectUTuple(mem, heap1, utuples1);
			}
			TwoPass.checkMemory(mem, 0, numsublist1, offset1, tablesublist1, relation_reference1,fieldkey, heap1);
		}
		//join remaining tuples
		outputUTuple(utuples1, utuples);
		return utuples;
	}
	
	public static ArrayList<UTuple> furtherPocessing(ArrayList<UTuple> input, Schema schema, ArrayList<String> attributes, String key) 
	{
		ArrayList<UTuple> output = new ArrayList<UTuple>();
		if (input.size() > 0 && key != null) {
			int index = attributes.indexOf(key);
			if (index == -1) {
				int i = schema.getFieldNames().indexOf(key);
				HashSet<UTuple> hash = new HashSet<UTuple>();
				for (UTuple ut : input)
				{	
					ArrayList<Field> fields = ut.fields();
					ArrayList<Field> fieldstemp = new ArrayList<Field>();
					for (int k = 0; k < fields.size(); k++){
						if (k != i) fieldstemp.add(fields.get(k));
					}
					if (hash.add(new UTuple(ut.key(), fieldstemp))) {
						output.add(ut);
					}
				}
				return output;
			}
		}
		return input;
	}
	
	private ArrayList<UTuple> OneTimeMeory(Relation relation_reference, String key) {
		ArrayList<UTuple> output = new ArrayList<UTuple>();
		HashSet<UTuple> heap1 = new HashSet<UTuple>();
		relation_reference.getBlocks(0, 0, relation_reference.getNumOfBlocks());
		for (int i = 0; i < relation_reference.getNumOfBlocks(); i++) {
			Block block_reference = mem.getBlock(i);
			for (Tuple tup : block_reference.getTuples()) {
				Field keyfield = tup.getField(key);
				heap1.add(new UTuple(keyfield, tup));
			}
		}
		output.addAll(heap1);
		return output;
	}
	
	private void outputUTuple(ArrayList<UTuple> utuples1,  ArrayList<UTuple> utuples)
	{	
		if (utuples1.size() != 0) {
			utuples.add(utuples1.get(0));
			utuples1.clear();
		}
	}
	private void collectUTuple(MainMemory mem, Heap<UTuple> heap, ArrayList<UTuple> utuples)
	{	
		UTuple u = heap.remove();
		utuples.add(u);
		TwoPass.InvalidateTuples(mem, u.blockindex, u.tupleindex);
	}
}
