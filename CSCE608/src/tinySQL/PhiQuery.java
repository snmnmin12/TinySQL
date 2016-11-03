package tinySQL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import storageManager.Block;
import storageManager.Disk;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class PhiQuery {
	private Parser2 parse;
	MainMemory mem;
	Disk disk;
	SchemaManager schema_manager;
	
	public PhiQuery(){
		parse = new Parser2();
		mem=new MainMemory();
		disk=new Disk();
		schema_manager=new SchemaManager(mem,disk);
	    disk.resetDiskIOs();
	    disk.resetDiskTimer();
	}
	public boolean execute(String statement) {
		parse.SyntaxParse(statement);
		if (parse.words.size() == 0)
			Parser2.error("Statements Can't be understood!");
		if ("create".equalsIgnoreCase(parse.words.get(0)))
			return createQuery();
		if ("insert".equalsIgnoreCase(parse.words.get(0)))
			return insertQuery();
		if ("select".equalsIgnoreCase(parse.words.get(0)))
			return selectQuery();
		return true;
	}
	
	public boolean createQuery() {
		if (parse.words.size() < 3)
			Parser2.error("Create Size is Wrong!");
		if (!"create".equalsIgnoreCase(parse.words.get(0)))
			Parser2.error("Create Keyword is Wrong!");
		if (!"table".equalsIgnoreCase(parse.words.get(1)))
			Parser2.error("Table Keyword be understood!");
		if ("".equals(parse.words.get(2)))
			Parser2.error("Table name is not given");
		if (parse.fields.size() == 0)
			Parser2.error("Fields size is wrong!");
		if (parse.fields.size() != parse.fieldtypes.size())
			Parser2.error("field size and fieldtypes does not match");
		
		String relation_name = parse.words.get(2).toLowerCase();
	    ArrayList<String> field_names= parse.fields;
	    ArrayList<FieldType> field_types= parse.fieldtypes;
	    Schema schema=new Schema(field_names,field_types);
	    field_names=schema.getFieldNames();
	    System.out.print(field_names.toString()+"\n");
	    System.out.print("The schema has field types: " + "\n");
	    field_types=schema.getFieldTypes();
	    System.out.print(field_types.toString()+"\n");
	    System.out.print("\n");
	    
	    //create relations
	    //System.out.print("Creating table " + relation_name + "\n");
	    Relation relation_reference=schema_manager.createRelation(relation_name,schema);
		return true;
	}
	
	public boolean insertQuery() {
		//error checking
		if (parse.words.size() < 3)
			Parser2.error("Insert Size is Wrong!");
		if (!"insert".equalsIgnoreCase(parse.words.get(0)))
			Parser2.error("Insert 'into' Keyword is Wrong!");
		if (!"into".equalsIgnoreCase(parse.words.get(1)))
			Parser2.error("Table Keyword can not be understood!");
		if ("".equals(parse.words.get(2)))
			Parser2.error("Table name is not given");
		if (parse.fields.size() == 0)
			Parser2.error("Fields size is wrong!");
		if (parse.fields.size() != parse.values.size())
			Parser2.error("field size and values size does not match");

		//relations insertion into the relations
		String relation_name = parse.words.get(2).toLowerCase();
		Relation relation_reference = schema_manager.getRelation(relation_name);
		//error checking
		if (relation_reference == null) 
			Parser2.error("Relation with this name can't be found");
		//create tuples
		Tuple tuple = relation_reference.createTuple();
//	    System.out.print("The tuple has schema" + "\n");
//	    System.out.print(tuple_schema + "\n\n");
	    
	    ArrayList<String> field_names= parse.fields;
	    ArrayList<Field>  values = parse.values;
	    
	    for (int i = 0 ; i < values.size(); i++) {
	    	Field f = values.get(i);
	    	if (f.type == FieldType.INT)
	    		tuple.setField(field_names.get(i), f.integer);
	    	else
	    		tuple.setField(field_names.get(i), f.str);
	    }
	    appendTupleToRelation(relation_reference, mem, 5, tuple);
	    System.out.print("Number of Blocks " + relation_reference.getNumOfTuples() +"\n");
	    System.out.print("Created a tuple " + tuple + " of " + relation_name +" through the relation" + "\n");
	    System.out.print("The tuple is invalid? " + (tuple.isNull()?"TRUE":"FALSE") + "\n");
	    System.out.print("\n");
		return true;
	}
	
	public boolean selectQuery() {
		
		if (parse.words.size() < 1)
			Parser2.error("Select Size is Wrong!");
		if (parse.select == null)
			Parser2.error("Select Node Can't be NULL!");
		if (!"select".equalsIgnoreCase(parse.words.get(0)))
			Parser2.error("Select Syntax is wrong!");
		if (!parse.select.from)
			Parser2.error("Select Syntax is wrong!");
		
		TreeNode select = parse.select;
		ArrayList<String> attributes = select.attributes;
		String[] tables = select.table;
		boolean distinct = select.distinct;
		OPTree optree = null;
		if (select.where) optree = select.conditions;
		
		//retrieve all the tuples in this relations and filter it
		//join will be implemented later
		//System.out.print(field_names.toString()+"\n");
		ArrayList<Tuple> tuples = new ArrayList<Tuple> ();
		for (String table: tables) 
		{
			Relation relation_reference = schema_manager.getRelation(table);
			System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
//			System.out.println("Now the relations is:");
//			System.out.println(relation_reference);
		    for (int i = 0; i < relation_reference.getNumOfBlocks(); i++) {
		    	relation_reference.getBlock(i,0);
		    	Block block_reference=mem.getBlock(0);
		    	for (Tuple tup: block_reference.getTuples()) {
		    		if (optree != null) { 
		    			if (optree.check(relation_reference.getSchema(), tup))
		    				tuples.add(tup);
		    		}
		    		else tuples.add(tup);
		    	}
		    }
		}
		
		//output the retrieved the results
//		System.out.println("The initial select all returned below");
		for (Tuple tup: tuples)
			System.out.println(tup);
		System.out.println("\n");
		
		return true;
	}
	
	  private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
		    Block block_reference;
		    if (relation_reference.getNumOfBlocks()==0) {
//		      System.out.print("The relation is empty" + "\n");
//		      System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
		      block_reference=mem.getBlock(memory_block_index);
		      block_reference.clear(); //clear the block
		      block_reference.appendTuple(tuple); // append the tuple
//		      System.out.print("Write to the first block of the relation" + "\n");
		      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
		    } else {
//		      System.out.print("Read the last block of the relation into memory block "+memory_block_index+" :" + "\n");
		      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
		      block_reference=mem.getBlock(memory_block_index);

		      if (block_reference.isFull()) {
//		        System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
		        block_reference.clear(); //clear the block
		        block_reference.appendTuple(tuple); // append the tuple
//		        System.out.print("Write to a new block at the end of the relation" + "\n");
		        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
		      } else {
//		        System.out.print("(The block is not full: Append it directly)" + "\n");
		        block_reference.appendTuple(tuple); // append the tuple
//		        System.out.print("Write to the last block of the relation" + "\n");
		        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
		      }
		    }
		  }
	
	public static void parseFile(String... files) throws IOException
	{
	    if(files.length == 0) Parser2.error("Error files");
//	    Parser2 parse = new Parser2();
	    PhiQuery query = new PhiQuery();
	    List<String> lines = new ArrayList<String>();
	    File file = new File(files[0]); //for ex foo.txt
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	boolean flag = query.execute(line);
		    	if (!flag)
		    		Parser2.error("Error in processing files files");
		    }
		}catch(IOException e) {
			System.out.println(e);
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String create ="CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
		String insert0 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")";
		String insert1 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 98, \"C\")";
		String insert2 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 69, 64, \"C\")";
		String insert3 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (15, 100, 50, 90, \"E\")";
		String insert4 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (15, 100, 99, 100, \"E\")";
		String insert5 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (17, 100, 100, 100, \"A\")";
		String select = "SELECT * FROM course where course.grade = \"A\"";
		String filename = "test.txt";
		PhiQuery query = new PhiQuery();
		//parseFile(filename);
		query.execute(create);
		query.execute(insert0);
		query.execute(insert1);
		query.execute(insert2);
		query.execute(insert3);
		query.execute(insert4);
		query.execute(insert5);
		query.execute(select);
	}

}
