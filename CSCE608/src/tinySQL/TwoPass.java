package tinySQL;

import java.util.ArrayList;

import storageManager.Block;
import storageManager.Field;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

public class TwoPass {
	public static ArrayList<Integer> FirstPass(Relation relation_reference, MainMemory mem, Relation relation_referencetemp, String fieldkey) {
		Heap<UTuple> heap = new Heap<UTuple>();
		ArrayList<Integer> record = new ArrayList<Integer>();
		
		int relationnum = relation_reference.getNumOfBlocks();
		int senttomem = mem.getMemorySize()>relationnum?relationnum:mem.getMemorySize();
		int alreadyinmem = 0;
		for (int i = 0; i < relationnum; i += senttomem) {
			if (i+senttomem > relationnum)
				senttomem = relationnum - i;
			relation_reference.getBlocks(i,0,senttomem);
			for (int j = 0; j < senttomem; j++) {
	    		Block block_reference = mem.getBlock(j);
	    		if (block_reference.getNumTuples() == 0) continue;
				for (Tuple tup : block_reference.getTuples()) {
					if (tup.isNull()) continue;
					String keytemp = fieldkey;
					if (!tup.getSchema().fieldNameExists(fieldkey))
						keytemp =  fieldkey.substring(fieldkey.indexOf('.')+1);
					Field key = tup.getField(keytemp);
					heap.insert(new UTuple(key, tup));
				}
			}
			ArrayList<UTuple> utuples = heap.Build();
			ArrayList<Tuple> tuples = UTuple.UtoTuples(relation_referencetemp, utuples);
			RelationHelper.appendMemToRelation(relation_referencetemp, mem, tuples);
			alreadyinmem += senttomem;
			record.add(alreadyinmem);
		}
		return record;
	}
	public static void checkMemory(MainMemory mem, int start, int num1, ArrayList<Integer> offset1, ArrayList<Integer> tablelist1,Relation relation_reference1, 
			String attri, Heap<UTuple> heap1) {
		for (int i = 0; i < num1; i++) 
			if (mem.getBlock(i+start).getNumTuples() == 0) {
				if (offset1.get(i) < tablelist1.get(i)) {
					relation_reference1.getBlock(offset1.get(i), i+start);
					offset1.set(i,offset1.get(i)+1);
				}
				Block block_reference = mem.getBlock(i+start);
				if (block_reference.getNumTuples() == 0)
					continue;
				for (int j = 0; j < block_reference.getNumTuples(); j++) {
					Tuple tup = block_reference.getTuple(j);
					String keytemp = tup.getSchema().fieldNameExists(attri)?attri:attri.substring(attri.indexOf('.')+1);
					Field key = tup.getField(keytemp);
					heap1.insert(new UTuple(key, tup, i+start, j));
				}
			}
	}
	public static void InvalidateTuples(MainMemory mem, int blockindex, int tupleindex) {
		mem.getBlock(blockindex).invalidateTuple(tupleindex);
	}
	//to remove tuples in main memeory and remove tuples when it meets certain requirements
	public static void RemoveCollectTuples(MainMemory mem, Heap<UTuple> heap, ArrayList<UTuple> utuples) {
		UTuple u = heap.remove();
		utuples.add(u);
		InvalidateTuples(mem, u.blockindex, u.tupleindex);
	}
}
