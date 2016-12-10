package tinySQL;


import java.util.ArrayList;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class UTuple implements Comparable<UTuple>{
	private final  Field key;
	private final ArrayList<Field> fields;
	public int blockindex = 0;
	public int tupleindex = 0;
	
//	public UTuple() {}
	public UTuple(Field key, Tuple tuple) {
		this.key = key;
		this.fields = new ArrayList<Field>();
		for (int i = 0; i < tuple.getNumOfFields(); i++) 
		fields.add(tuple.getField(i));
	}	
	
	public UTuple(Field key, Tuple tuple, int blockindex, int tupleindex) {
		this.key = key;
		this.fields = new ArrayList<Field>();
		for (int i = 0; i < tuple.getNumOfFields(); i++) 
		fields.add(tuple.getField(i));
		this.blockindex = blockindex;
		this.tupleindex = tupleindex;
	}	
	
	public UTuple(ArrayList<Field> fields) {
		this.fields = fields;
		this.key = new Field();
	}
	
	public UTuple(Field key, ArrayList<Field> fields) {
		this.fields = fields;
		this.key = key;
	}
	
	public UTuple JoinUTuple(UTuple ut2) {
		ArrayList<Field> newfields = new ArrayList<Field>();
		newfields.addAll(fields);
		newfields.addAll(ut2.fields);
		return new UTuple(key, newfields);
	}
	
	public ArrayList<Field> fields() {
		return fields;
	}
	
	 public Field key()  {
		 return key;
	 }
	
	 public String toString()  {
	  String str = "";
	  for (int i=0;i<fields.size();i++)
	      str+=fields.get(i)+"\t";
	  return str;
	}
	
	@Override
	public int compareTo(UTuple key2) {
		// TODO Auto-generated method stub
		if (key.type == FieldType.INT)
			return  ((Integer)key.integer).compareTo(key2.key.integer);
		if (key.type == FieldType.STR20)
			return  key.str.compareTo(key2.key.str);
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof UTuple))
			return false;
		return this.hashCode() == obj.hashCode();
	}
	//to make this class hashable, so it can be used by hashmap
	public int hashCode() {
		String str = "";
		for (Field f:fields)
			str += f;
		return str.hashCode();
	}
	
	public static ArrayList<Tuple> UtoTuples(Relation relation_reference, ArrayList<UTuple>input) {
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (UTuple ut : input) {
			Tuple tuple = relation_reference.createTuple();
			for (int i = 0; i < ut.fields().size(); i++) {
				if (ut.fields().get(i).type == FieldType.INT)
					tuple.setField(i, ut.fields().get(i).integer);
				else 
					tuple.setField(i, ut.fields().get(i).str);
			}
			tuples.add(tuple);
		}
		return tuples;
	}
	public static Schema buildSchema(Tuple tup, ArrayList<String> attributes, String select_order_by) 
	{
		ArrayList<FieldType> types =  new ArrayList<FieldType>();
		Schema schema = tup.getSchema();
		ArrayList<String> temp = new ArrayList<String>();
		if (select_order_by != null && attributes.indexOf(select_order_by) == -1) temp.add(select_order_by);
		temp.addAll(attributes);
		for (String field: temp) {
			if (schema.fieldNameExists(field)) {
				types.add(tup.getField(field).type);
			}
			else {
				field =field.substring(field.indexOf('.')+1);
				types.add(tup.getField(field).type);
			}
		}
		schema = new Schema(temp, types);
		return schema;
	}
	
	public static ArrayList<Tuple> ShrinkTuples(Relation relationname, ArrayList<Tuple> tuples) 
	{
		ArrayList<Tuple> output = new ArrayList<Tuple>();
		for (Tuple tup: tuples) {
			Tuple tuple = relationname.createTuple();
			for (String field:tuple.getSchema().getFieldNames()) {
				Field f = null;
				if (!tup.getSchema().fieldNameExists(field))
						f = tup.getField(field.substring(field.indexOf('.')+1));
				else f = tup.getField(field);
				if (f.type == FieldType.INT)
					tuple.setField(field, f.integer);
				else 
					tuple.setField(field, f.str);
			}
			output.add(tuple);
		}
		return output;
	}

	public static ArrayList<UTuple> TupletoUT(ArrayList<Tuple> tuples, String fieldname) 
	{
		ArrayList<UTuple> output = new ArrayList<UTuple>();
		for (Tuple tup: tuples) {
			Field key = tup.getField(fieldname);
			output.add(new UTuple(key,tup));
		}
		return output;
	}
}
