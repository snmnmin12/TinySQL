package tinySQL;


import java.util.ArrayList;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Schema;
import storageManager.Tuple;

public class UTuple implements Comparable<UTuple>{
	private final  Field key;
	private final ArrayList<Field> fields;
//	public UTuple() {}
	public UTuple(Field key, Tuple tuple) {
		this.key = key;
		this.fields = new ArrayList<Field>();
		for (int i = 0; i < tuple.getNumOfFields(); i++) 
		fields.add(tuple.getField(i));
	}	
	public UTuple(ArrayList<Field> fields) {
		this.fields = fields;
		this.key = new Field();
	}
	
	public UTuple(Field key, ArrayList<Field> fields) {
		this.fields = fields;
		this.key = key;
	}

	public ArrayList<Field> fields() {
		return fields;
	}
	
//	public String toString() {
//		return fields.toString();
//	}
	
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
}
