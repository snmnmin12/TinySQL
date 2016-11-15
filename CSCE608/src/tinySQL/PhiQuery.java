package tinySQL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import storageManager.*;

public class PhiQuery {
	
	private Parser2 parse;
	public MainMemory mem;
	public Disk disk;
	public SchemaManager schema_manager;
	public HashSet<UTuple> hash;
	public Heap<UTuple> heap;
	
	public PhiQuery(){
		//this is the constructor for execute the physical queries
		parse = new Parser2();
		mem=new MainMemory();
		disk=new Disk();
		hash = null;
		heap = null;
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
		else if ("insert".equalsIgnoreCase(parse.words.get(0)))
			return insertQuery();
		else if ("select".equalsIgnoreCase(parse.words.get(0))) {
			selectQuery();
			return true;
		}
		else if ("delete".equalsIgnoreCase(parse.words.get(0))) {
			return deleteQuery();
		}else if ("drop".equalsIgnoreCase(parse.words.get(0))) {
			return dropQuery();
		}else if ("source".equalsIgnoreCase(parse.words.get(0))) {
			System.out.println("I am here to process files!");
			return true;
		} else {
			return false;
		}
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
	    //create relations
	    //System.out.print("Creating table " + relation_name + "\n");
	    schema_manager.createRelation(relation_name,schema);
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

		//relations insertion into the relations
		String relation_name = parse.words.get(2).toLowerCase();
		Relation relation_reference = schema_manager.getRelation(relation_name);
		//error checking
		if (relation_reference == null) 
			Parser2.error("Relation with this name can't be found");
		//create tuples
		Tuple tuple = relation_reference.createTuple();
//	    System.out.print("The tuple has schema" + "\n");
	    
	    ArrayList<String> field_names= parse.fields;
	    ArrayList<Field>  values = parse.values;
	    if (values.size() != 0) {
		    for (int i = 0 ; i < values.size(); i++) {
		    	Field f = values.get(i);
		    	if (f.type == FieldType.INT)
		    		tuple.setField(field_names.get(i), f.integer);
		    	else {
		    		if(!f.str.equalsIgnoreCase("NULL"))
		    			tuple.setField(field_names.get(i), f.str);
		    		else 
		    			tuple.setField(field_names.get(i), 0);
		    		}
		    }
		    appendTupleToRelation(relation_reference, mem, 5, tuple);
	    }else {
	    		if (parse.select ==null)
	    			Parser2.error("Error in Update for select");
	    		for(Tuple tu:selectQuery()) {
	    			tuple = tu;
	    			appendTupleToRelation(relation_reference, mem, 5, tuple);
	    		}
	    }
//	    System.out.print("Number of Blocks " + relation_reference.getNumOfTuples() +"\n");
//	    System.out.print("Created a tuple " + tuple + " of " + relation_name +" through the relation" + "\n");
//	    System.out.print("The tuple is invalid? " + (tuple.isNull()?"TRUE":"FALSE") + "\n");
//	    System.out.print("\n");
		return true;
	}
	
	public ArrayList<Tuple> selectQuery() {
		
		if (parse.words.size() < 1)
			Parser2.error("Select Size is Wrong!");
		if (parse.select == null)
			Parser2.error("Select Node Can't be NULL!");
		if (!parse.select.from)
			Parser2.error("Select Syntax is wrong!");
		
		//check if the selected results should be pipelined or materialized
		hash = null;
		heap = null;
		boolean pipeline = "select".equalsIgnoreCase(parse.words.get(0));
		if (pipeline)
			System.out.println(parse.sentence+" started!");
		TreeNode select = parse.select;
		String[] finaltables = select.table;
		ArrayList<String> tables = new ArrayList<String>();
		for (String str:finaltables)
			tables.add(str);
		
		ExpressionTree Tree = null;
		if (select.where) 
			Tree = select.conditions;
		
		//optimizations, possible, first optimize the join operation
		ArrayList<String> temptables = Optimization.selectOptimization(finaltables, this, Tree);
		if ((temptables != null) && (temptables.size()!=0)) {
			if (temptables.size() != tables.size())  
				changeTables(tables, temptables);
			else tables = temptables;
		}

		ArrayList<UTuple>  output = excuteJoin(tables, select);
		if (heap != null)
		{
			output = heap.Build();
		}	
		
		if (pipeline)
		{
			for (UTuple res:output)
				System.out.println(res.toString());
			System.out.println(parse.sentence+" is over!"+"\n");
		}
		else 
			return toTuples(output, select.attributes);
		removeTempRelations(tables, finaltables);
		return null;
	}
	
	//execute the drop statements
	public boolean dropQuery() {
		if (parse.words.size() < 1)
			Parser2.error("Drop Size is Wrong!");
		if (!"drop".equalsIgnoreCase(parse.words.get(0)))
			Parser2.error("Drop Syntax is wrong!");
		if (!"table".equalsIgnoreCase(parse.words.get(1)))
			Parser2.error("Drop Syntax is wrong!");
		String table = parse.words.get(2);
		schema_manager.deleteRelation(table);
		return true;
	}
	
	public boolean deleteQuery() {
		//to delete tuples from database with certain conditions
		if (parse.words.size() < 1)
			Parser2.error("Select Size is Wrong!");
		if (!"delete".equalsIgnoreCase(parse.words.get(0)))
			Parser2.error("Select Syntax is wrong!");
		if (!parse.delete.from)
			Parser2.error("Select Syntax is wrong!");
		
		TreeNode delete = parse.delete;
		String table = delete.table[0];
		tableScan(table,delete, 1, null);
		
		return true;
	}
	
	
	//***********************************************************************************************//
	//below are helper method for execute the above methods
	
	//to execute join of many tables, after that read all tuples from one relations using table scan
	private ArrayList<UTuple> excuteJoin(ArrayList<String> tables, TreeNode select) {
		String pretable = null;
		String curtable = null;
		ExpressionTree tree = select.conditions;
		//handle join
		if (select.distinct && hash == null)
			hash = new HashSet<UTuple>();
		if (select.order_by != null && heap == null)
			heap = new Heap<UTuple>();
		
		ArrayList<ExpressionTree> allnodes = null;
		if (tree!=null) allnodes = tree.hasSelection();
		
		String[] alltables = tables.toArray(new String[tables.size()]);
		for (String table: alltables) 
		{
			curtable = table;
			if (pretable != null) {
				pretable = Join.JoinTable2(this, pretable, curtable,allnodes);
				tables.add(pretable);
			}else {
				pretable = curtable;
			}
		}
		curtable = pretable;
		ArrayList<UTuple> tuples = tableScan(curtable, select, 0, select.attributes);
		//ArrayList<UTuple> output = distinctOperation(curtable, tuples, select);
		return tuples;
	}
	//read the relation into memory and then execute statements one by one, try to maximize the reading
	//action 0 for select, 1 for delete
	private ArrayList<UTuple> tableScan(String table, TreeNode selecdeletetree, int action, ArrayList<String> attris) {
		//System.out.print(relation_reference.getSchema().fieldNamesToString()+"\n");
		Relation relation_reference = schema_manager.getRelation(table);
		int memnumBlocks = mem.getMemorySize();
		ArrayList<UTuple> tuples = new ArrayList<UTuple> ();

		//retrieve blocks in two pass because the memory size is only 10 blocks
		int relationnumBlocks = relation_reference.getNumOfBlocks();
		int alreadyreadblocks = 0;
		while (relationnumBlocks > 0) {
			int senttomem = memnumBlocks > relationnumBlocks?relationnumBlocks:memnumBlocks;
			relation_reference.getBlocks(alreadyreadblocks,0,senttomem);
			if (action == 0)
				selectTuples(relation_reference, senttomem, selecdeletetree, tuples, attris);
			else if (action == 1)
				deleteTuples(relation_reference, senttomem, selecdeletetree, alreadyreadblocks);
		    relationnumBlocks -= senttomem;
		    alreadyreadblocks += senttomem;
		}
		return tuples;
	}
	
	//execute the select tuples according to conditions in memory
	private void selectTuples(Relation relation_reference, int senttomem, TreeNode select, ArrayList<UTuple> output, ArrayList<String> attributes)
	{
		ExpressionTree tree = select.conditions;
		
		if ("*".equalsIgnoreCase(attributes.get(0))) {
			attributes.clear();
			attributes.addAll(relation_reference.getSchema().getFieldNames());
		}	
	    for (int i = 0; i < senttomem; i++) {
	    	Block block_reference=mem.getBlock(i);
	    	//this is to handle the holes after deletion
	    	if (block_reference.getNumTuples() == 0) continue;
	    	for (Tuple tup: block_reference.getTuples()) {
	    		if (tree != null && tree.check(relation_reference.getSchema(), tup)) {
	    				//tuples.add(tup);
	    			distinctOp(tup, output, select, attributes); 
	    		}
	    		else if (tree == null) {
	    			//tuples.add(tup);
	    			distinctOp(tup, output, select, attributes);
	    		}
	    	}
	    }
	}
	//UTuple to tuple
	private ArrayList<Tuple> toTuples(ArrayList<UTuple> output, ArrayList<String> attributes) {
		String tablename = "totuples";
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for (int i = 0; i < output.get(0).fields().size(); i++) 
			field_types.add(output.get(0).fields().get(i).type);
		Schema schema=new Schema(attributes,field_types);
		Relation relation_reference = schema_manager.createRelation(tablename, schema);
		
		for (UTuple ut : output) {
			Tuple tuple = relation_reference.createTuple();
			for (int i = 0; i < ut.fields().size(); i++) {
				if (ut.fields().get(i).type == FieldType.INT)
					tuple.setField(i, ut.fields().get(i).integer);
				else 
					tuple.setField(i, ut.fields().get(i).str);
			}
			tuples.add(tuple);
		}
		//schema_manager.deleteRelation(tablename);
		return tuples;
	}
	//delete blocks of relation in memory and then write it back to disk
	private void deleteTuples(Relation relation_reference, int senttomem, TreeNode delete, int alreadyreadblocks) {
		ExpressionTree tree =delete.conditions;
	    for (int i = 0; i < senttomem; i++) {
	    	Block block_reference=mem.getBlock(i);
	    	//this is to handle the holes after deletion
	    	if (block_reference.getNumTuples() == 0)
	    		continue;
	    	ArrayList<Tuple> tuples = block_reference.getTuples();
	    	for (int j = 0; j < tuples.size(); j++) {
	    		if (tree != null)
	    		{ 
	    			if (tree.check(relation_reference.getSchema(), tuples.get(j)))
	    			block_reference.invalidateTuple(j);
	    		}
	    		else 
	    			block_reference.invalidateTuples();
	    	}
	    }
	    relation_reference.setBlocks(alreadyreadblocks, 0, senttomem);
	}
		
	private void distinctOp(Tuple tup, ArrayList<UTuple> output, TreeNode select, ArrayList<String> attributes) {
		
		//output the retrieved the results
		ArrayList<Field> res = new ArrayList<Field>(); 
		Schema schema = tup.getSchema();
		for (String field: attributes) {
			//handle cases like table.attribute
			if (schema.fieldNameExists(field))
				res.add(tup.getField(field));
			else 
				res.add(tup.getField(field.substring(field.indexOf('.')+1)));
		}
		//handle the distinct
		if ((hash == null) ||(hash!=null && hash.add(new UTuple(res)))) {
			output.add(new UTuple(res));
			//handle order_by
			if (heap != null) {
				Field f = tup.getField(select.order_by);
				heap.insert(new UTuple(f, res));
			}
		}
	}
	
	//to remove intermediate relations in disk
	private void removeTempRelations(ArrayList<String> tables, String[] finaltables) {
		for (String table:tables)
			if (!Arrays.asList(finaltables).contains(table))
				schema_manager.deleteRelation(table);
	}
	
	//change the tables
	private void changeTables(ArrayList<String> tables, ArrayList<String> temptables) {
		for (String temptable:temptables) {
			String table = temptable.substring(temptable.indexOf("temp")+"temp".length());
			int index = tables.indexOf(table);
			tables.set(index, temptable);
		}
	}
	
	public static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
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
	    //List<String> lines = new ArrayList<String>();
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
		
		System.out.println("File Parsing Completed!");
//		String select = "SELECT * FROM course where course.grade = \"A\"";
//		query.execute(select);
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		String create ="CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
//		String insert0 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, NULL, 100, 100, \"A\")";
//		String insert1 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 98, \"C\")";
//		String insert2 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 69, 64, \"C\")";
//		String insert3 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (15, 100, 50, 90, \"E\")";
//		String insert4 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (15, 100, 99, 100, \"E\")";
//		String insert5 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (17, 100, 100, 100, \"A\")";
//		String insert6 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (2, 100, 100, 99, \"B\")";
//		String insert7 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (4, 100, 100, 97, \"D\")";
//		String insert8 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (5, 100, 100, 66, \"A\")";
//		String insert9 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (6, 100, 100, 65, \"B\")";
//		String insert10 = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 100, 50, 73, \"C\")";
//		String select = "SELECT * FROM course";
//		String delete = "DELETE FROM course WHERE grade = \"E\"";
//		//String insert = "INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course";
//		String drop = "DROP TABLE course";
//		String select2 = "SELECT sid, grade FROM course";
		String filename = "test2.txt";
//		PhiQuery query = new PhiQuery();
		parseFile(filename);
//		query.execute(create);
//		query.execute(insert0);
//		query.execute(insert1);
//		query.execute(insert2);
//		query.execute(insert3);
//		query.execute(insert4);
//		query.execute(insert5);
//		query.execute(insert6);
//		query.execute(insert7);
//		query.execute(insert8);
//		query.execute(insert9);
//		query.execute(insert10);
//		query.execute(delete);
//		//query.execute(drop);
//		query.execute(select2);
	}

}
