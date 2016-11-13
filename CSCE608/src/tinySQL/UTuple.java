package tinySQL;

import java.util.ArrayList;

import storageManager.Field;
import storageManager.Tuple;

public class UTuple {
	private ArrayList<String> fieldnames;
	private ArrayList<Field> fields; // stores int and string fields
	public UTuple(Tuple tuple) {
		this.fieldnames = new ArrayList<String>(tuple.getSchema().getFieldNames());
		this.fields = new ArrayList<Field>();
		for (int i = 0; i < tuple.getNumOfFields(); i++) 
			fields.add(tuple.getField(i));
	}
	public UTuple() {
		this.fieldnames = new ArrayList<String>();
		this.fields = new ArrayList<Field>();
	}
	public UTuple(ArrayList<String> fieldnames) {
		this.fieldnames = fieldnames;
		this.fields = new ArrayList<Field>();
	}
	public UTuple(ArrayList<String> fieldnames, ArrayList<Field> fields) {
		this.fieldnames = fieldnames;
		this.fields = fields;
	}
	public ArrayList<String> getNames() {
		return  fieldnames;
	}
	public ArrayList<Field> getFields() {
		return fields;
	}
	public void addName(String fieldname) {
		fieldnames.add(fieldname);
	}
	public void addField(Field f) {
		fields.add(f);
	}
	public void addName(ArrayList<String> fieldnames) {
		for (String fieldname: fieldnames)
			fieldnames.add(fieldname);
	}
	public void addField(ArrayList<Field> fs) {
		for (Field f:fs)
			fields.add(f);
	}
	public Field getField(String field_name) {
		int i = 0;
		for (;i < fieldnames.size(); i++)
			if (field_name.equalsIgnoreCase(fieldnames.get(i))) {
				break;
			}
		return fields.get(i);
	}
}
