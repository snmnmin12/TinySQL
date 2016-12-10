package tinySQL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import storageManager.*;
/*
 * @author: Mingmin Song
 */
public class PhiQuery {
	
	public Parser2 parse;
	public MainMemory mem;
	public Disk disk;
	public SchemaManager schema_manager;
	public HashSet<UTuple> hash;
	public Heap<UTuple> heap;
	public boolean pipeline;
	public PrintStream out;
	
	public PhiQuery(){
		//this is the constructor for execute the physical queries
		parse = null;
		mem=new MainMemory();
		disk=new Disk();
		hash = null;
		heap = null;
		pipeline = true;
		schema_manager=new SchemaManager(mem,disk);
	    disk.resetDiskIOs();
	    disk.resetDiskTimer();
	}
	
	public boolean execute(String statement) throws IOException, ParserException {
		parse = new Parser2();
		parse.SyntaxParse(statement);
		if (parse.words.size() == 0)
			throw new ParserException("Statements Can't be understood!");
		if ("create".equalsIgnoreCase(parse.words.get(0)))
			return createQuery();
		else if ("insert".equalsIgnoreCase(parse.words.get(0)))
			return insertQuery();
		else if ("select".equalsIgnoreCase(parse.words.get(0))) {
			long start = System.currentTimeMillis();
			selectQuery();
			long end = System.currentTimeMillis();
			NumberFormat formatter = new DecimalFormat("#0.00");
			System.out.println("Execution time: "+formatter.format((end - start) / 1000d)+" seconds\n");
			return true;
		}
		else if ("delete".equalsIgnoreCase(parse.words.get(0))) {
			return deleteQuery();
		}else if ("drop".equalsIgnoreCase(parse.words.get(0))) {
			return dropQuery();
		}else if ("source".equalsIgnoreCase(parse.words.get(0))) {
			String[] files = parse.words.subList(1,parse.words.size()) .toArray(new String[parse.words.size()-1]);
			return parseFile(files);
		} else {
			return false;
		}
	}
	
	public boolean createQuery() throws ParserException {
		if (parse.words.size() < 3)
			throw new ParserException("Create Size is Wrong!");
		if (!"create".equalsIgnoreCase(parse.words.get(0)))
			throw new ParserException("Create Keyword is Wrong!");
		if (!"table".equalsIgnoreCase(parse.words.get(1)))
			throw new ParserException("Table Keyword be understood!");
		if ("".equals(parse.words.get(2)))
			throw new ParserException("Table name is not given");
		if (parse.fields.size() == 0)
			throw new ParserException("Fields size is wrong!");
		if (parse.fields.size() != parse.fieldtypes.size())
			throw new ParserException("field size and fieldtypes does not match");
		
		String relation_name = parse.words.get(2).toLowerCase();
	    ArrayList<String> field_names= parse.fields;
	    ArrayList<FieldType> field_types= parse.fieldtypes;
	    Schema schema=new Schema(field_names,field_types);	    
	    //create relations
	    //System.out.print("Creating table " + relation_name + "\n");
	    schema_manager.createRelation(relation_name,schema);
		return true;
	}
	
	public boolean insertQuery() throws ParserException {
		//error checking
		if (parse.words.size() < 3)
			throw new ParserException("Insert Size is Wrong!");
		if (!"insert".equalsIgnoreCase(parse.words.get(0)))
			throw new ParserException("Insert 'into' Keyword is Wrong!");
		if (!"into".equalsIgnoreCase(parse.words.get(1)))
			throw new ParserException("Table Keyword can not be understood!");
		if ("".equals(parse.words.get(2)))
			throw new ParserException("Table name is not given");
		if (parse.fields.size() == 0)
			throw new ParserException("Fields size is wrong!");

		//relations insertion into the relations
		String relation_name = parse.words.get(2).toLowerCase();
		Relation relation_reference = schema_manager.getRelation(relation_name);
		//error checking
		if (relation_reference == null) 
			throw new ParserException("Relation with this name can't be found");
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
		    RelationHelper.appendTupleToRelation(relation_reference, mem, 5, tuple);
	    }else {
    		if (parse.select ==null)
    			throw new ParserException("Error in Update for select");
    		for(Tuple tu:selectQuery()) {
    			tuple = tu;
    			RelationHelper.appendTupleToRelation(relation_reference, mem, 5, tuple);
    		}
	    }
		return true;
	}
	
	public ArrayList<Tuple> selectQuery() throws ParserException {
		
		if (parse.words.size() < 1)
			throw new ParserException("Select Size is Wrong!");
		if (parse.select == null)
			throw new ParserException("Select Node Can't be NULL!");
		if (!parse.select.from)
			throw new ParserException("Select Syntax is wrong!");
		if (parse.select.table == null || parse.select.table.length == 0)
			throw new ParserException("Table size is wrong!");
		//check if the selected results should be pipelined or materialized
		hash = null;
		heap = null;
		pipeline = "select".equalsIgnoreCase(parse.words.get(0));
		
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

		//removeTempRelations(tables, finaltables);
		if (pipeline)
		{
			if (output.size() > 0) {
			System.out.println("----------------------------------------------------");
			outputFields(select.attributes,finaltables);
			System.out.println("----------------------------------------------------");
			for (UTuple res: output) System.out.println(res);
			System.out.println("----------------------------------------------------");
			}
			System.out.println(output.size()+" rows in set!");
//			System.out.println(parse.sentence+" is over!"+"\n");
		}
		else {
			removeTempRelations(tables, finaltables);
			return toTuples(tables, output, select.attributes);
		}
		
		removeTempRelations(tables, finaltables);
		return null;
	}
	
	//execute the drop statements
	public boolean dropQuery() throws ParserException {
		if (parse.words.size() < 2)
			throw new ParserException("Drop Size is Wrong!");
		if (!"drop".equalsIgnoreCase(parse.words.get(0)))
			throw new ParserException("Drop Syntax is wrong!");
		if (!"table".equalsIgnoreCase(parse.words.get(1)))
			throw new ParserException("Drop Syntax is wrong!");
		String table = parse.words.get(2);
		schema_manager.deleteRelation(table);
		return true;
	}
	
	public boolean deleteQuery() throws ParserException {
		//to delete tuples from database with certain conditions
		if (parse.words.size() < 1)
			throw new ParserException("Select Size is Wrong!");
		if (!"delete".equalsIgnoreCase(parse.words.get(0)))
			throw new ParserException("Select Syntax is wrong!");
		if (!parse.delete.from)
			throw new ParserException("Select Syntax is wrong!");
		TreeNode delete = parse.delete;
		String table = delete.table[0];
		new SingleTableScan(schema_manager, mem).tableScan(table,delete, 1);
		
		return true;
	}
	
	
	//***********************************************************************************************//
	//below are helper method for execute the above methods
	//check if the tables are natural join 
	private boolean isAttri(ArrayList<String> tables, String attri) {
		for (String table: tables) {
			Schema schema = schema_manager.getSchema(table);
			if (schema.fieldNameExists(attri))
				return true;
			if (attri.indexOf('.') != -1) {
				if (schema.fieldNameExists(attri.substring(attri.indexOf('.')+1)))
					return true;
			}
		}
		return false;
	}
	private boolean isNaturalJoin(ArrayList<String> tables, TreeNode select, ArrayList<String> keys) {
		if (select.conditions == null)
			return false;
		ArrayList<ExpressionTree> trees = select.conditions.hasSelection();
		for (ExpressionTree tree:trees) {
			if (tree.getOp().equals("=")) {
				String leftop  =  tree.getLeft().getOp();
				String rightop =  tree.getRight().getOp();
				if (isAttri(tables, leftop) && isAttri(tables,rightop)) {
					keys.add(leftop);
					keys.add(rightop);
					return true;
				}
			}
		}
		return false;
	}
	//check if all blocks can be brought to memeory
	private boolean allInMemory(ArrayList<String> tables) {
		int i = 0;
		int numblocks = 0;
		for (; i < tables.size(); i++) {
			Relation relation_reference = schema_manager.getRelation(tables.get(i));
			numblocks += relation_reference.getNumOfBlocks();
		}
		if (numblocks <= mem.getMemorySize() && tables.size() > 3)
			return true;
		return false;
	}
	//to execute join of many tables, after that read all tuples from one relations using table scan
	private ArrayList<UTuple> excuteJoin(ArrayList<String> tables, TreeNode select) {
		
		Join join = new Join(this, select.conditions);
	
		if (select.distinct && hash == null)
			hash = new HashSet<UTuple>();
		if (select.order_by != null && heap == null)
			heap = new Heap<UTuple>();
		//to join many tables 
		ArrayList<Tuple> jointuples = null;
		ArrayList<String> keys = new ArrayList<String>();
		boolean flag = false;
		if (tables.size() >1) flag = isNaturalJoin(tables, select, keys);
		if (allInMemory(tables))
			jointuples = join.JoinManyTables(tables);
		else if (tables.size() == 2) 
		{
			if (flag) jointuples = join.NaturalJoin2Tables(tables, keys);
			else jointuples =  join.CrossJoin2Tables(tables);
		} else if (tables.size() == 3)
		{
			jointuples = join.CrossJoin3Tables(tables);
		}else {
			jointuples = join.defaultTableJoin(tables, select);
		}
		ArrayList<UTuple> tuples = new ArrayList<UTuple>() ;
		tuples = JoinedTableProcessing(tables, jointuples, select, select.attributes);
		return tuples;
	}
	
	//to process the tables after join
	ArrayList<UTuple> JoinedTableProcessing(ArrayList<String>tables, ArrayList<Tuple> jointuples, TreeNode select, ArrayList<String> attributes) {
		ExpressionTree tree = select.conditions;
		ArrayList<UTuple> output = new ArrayList<UTuple>();
		if (jointuples.size() == 0)
			return output;
		if ("*".equalsIgnoreCase(attributes.get(0))) {
			attributes.clear();
			attributes.addAll(jointuples.get(0).getSchema().getFieldNames());
		}
		ArrayList<Tuple> temp = new ArrayList<Tuple>();
		for (Tuple tup: jointuples) {
    		if (tree != null && tree.check(tup.getSchema(), tup)) {
//    			distinctOp(tup, output, select, attributes); 
    			temp.add(tup);
    		}else if (tree == null) {
//    			distinctOp(tup, output, select, attributes);
    			temp.add(tup);
			}
		}
		output = distinctOp(tables, temp, select, attributes);
//		RelationHelper.appendMemToRelation(relation_reference, mem, tuples);
		return output;
	}
	private ArrayList<UTuple> distinctOp(ArrayList<String>tables, ArrayList<Tuple> temp,TreeNode select,  ArrayList<String> attributes) {

		ArrayList<UTuple> output = new ArrayList<UTuple>();
		Tuple tup = temp.get(0);
		Schema schema = UTuple.buildSchema(tup, attributes, select.order_by);
		String relationname = "distinct1";
		tables.add(relationname);
		Relation relation_temp =  schema_manager.createRelation(relationname, schema);
		ArrayList<Tuple> tuples = UTuple.ShrinkTuples(relation_temp, temp);
		if (hash != null) {
			RelationHelper.appendMemToRelation(relation_temp, mem, tuples);
			int index = 0;
			if (select.order_by != null)
				index = schema.getFieldOffset(select.order_by);
			output = new DistinctOperation(schema_manager, mem).distinct(tables, relation_temp, schema.getFieldName(index));
			output = DistinctOperation.furtherPocessing(output, schema, attributes, select.order_by); 
		}
		else {
			int index = 0;
			if (select.order_by != null)
				index = schema.getFieldOffset(select.order_by);
			output = UTuple.TupletoUT(tuples, schema.getFieldName(index));
		}
		if (heap != null) {
			for (UTuple ut: output){
				ArrayList<Field> fields = ut.fields();
				ArrayList<Field> subfields = new ArrayList<Field>();
				Field key = null;
				int index = attributes.indexOf(select.order_by);
				if (index == -1) {
					subfields.addAll(fields.subList(1, fields.size()));
					key = subfields.get(0);
				}else  {
					subfields.addAll(fields);
					int i =   schema.getFieldNames().indexOf(select.order_by);
					key = fields.get(i); 
				}
				heap.insert(new UTuple(key, subfields));
			}
			output = heap.Build();
		}
		return output;
	}
	
	//UTuple to tuple
	private ArrayList<Tuple> toTuples(ArrayList<String> tables, ArrayList<UTuple> output, ArrayList<String> attributes) {
		String tablename = "totuples";
		if (pipeline)
			tables.add(tablename);
		else 
			tablename = parse.words.get(2).toLowerCase();
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		
		for (int i = 0; i < output.get(0).fields().size(); i++) 
			field_types.add(output.get(0).fields().get(i).type);
		Schema schema=new Schema(attributes,field_types);
		
		Relation relation_reference = null;
		if (pipeline)
			relation_reference = schema_manager.createRelation(tablename, schema);
		else 
			relation_reference = schema_manager.getRelation(tablename);
		
		ArrayList<Tuple> tuples = UTuple.UtoTuples(relation_reference, output);
		return tuples;
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
	
	//check if we can pipeline 
	public boolean isOutput() {
		if (pipeline && heap == null && hash == null)
			return false;
		return true;
	}
	
	//output the fieldnames 
	public void outputFields(ArrayList<String> attributes, String[] tables) {
		String str = "";
		if (attributes.size() == 1 && attributes.get(0).equals("*")) {
			for (int i = 0; i < tables.length; i++) {
				Relation relation_reference = schema_manager.getRelation(tables[i]);
				for (String field: relation_reference.getSchema().getFieldNames()) {
					if (tables.length > 1)
						str += tables[i]+"." + field;
					else 
					str += field;
					str += ", ";
				}
			}
			str = str.substring(0, str.lastIndexOf(','));
			System.out.println(str);
			return;
		}
		for (int i = 0; i < attributes.size(); i++) {
			str += attributes.get(i) ;
			if (i != attributes.size()-1) 
				str += ", ";
		}
		System.out.println(str);
	}
	
	
	public boolean parseFile(String... files) throws IOException, ParserException
	{
	    if(files.length == 0) 
	    	throw new ParserException("Error files");
	    File file = new File(files[0]); //for ex foo.txt
//	    PrintWriter writer = null;
	    if (files.length == 2)
	    	System.setOut(new PrintStream(new File(files[1])));
	    else if (files.length > 2)
	    	throw new ParserException("To many files!");
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	boolean flag = execute(line);
		    	if (!flag)
		    		throw new ParserException("Error in processing files files");
		    }
		}catch(IOException e) {
			System.out.println(e);
		}
		System.setOut(System.out);
		System.out.println("File Parsing Completed!");
		return true;
//		String select = "SELECT * FROM course where course.grade = \"A\"";
//		query.execute(select);
	}
	
	public static void main(String[] args) throws IOException, ParserException {
		// TODO Auto-generated method stub
		String filename = "source usertest.txt";

//		final long startTime = System.currentTimeMillis();
		PhiQuery query = new PhiQuery();
//		String create = "CREATE TABLE carMaker (id INT, name STR20, country STR20, manufactuer STR20,address STR20)";
//		String insertion = "INSERT INTO carMaker (id, name, country, manufactuer,address) VALUES (200000001,\"Audi\",\"Germany\",\"Volkswagen Group\",\"Ingolstadt Germany\")";
//		String select = "select * from carMaker";
//		query.execute(create);
//		query.execute(insertion);
//		query.execute(select);
		query.execute(filename);
//	    System.out.print("Calculated elapse time = " + query.disk.getDiskTimer() + " ms" + "\n");
//	    System.out.print("Calculated Disk I/Os = " + query.disk.getDiskIOs() + "\n");
//		final long endTime = System.currentTimeMillis();
//		System.out.println("Total execution time: " + (endTime - startTime) );
	}

}
