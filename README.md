# Tiny SQL
This is for the TinySQL project

1. Run the program, simply type the command 

	```
	java -jar TinySQL.jar
	```
	and the command line user command will show.

2. User can type the SQL command 

	```
    CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
    INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
    SELECT * FROM course
    SELECT sid, grade FROM course where grade = "A"
   
	```
	It will execute the above command like interprete 	SQL command. Also, user might want to process text 	files containing all the SQL command, such as 

	```
	source test.txt
		
	```
It will process all the SQL command in the test.txt files and then output the result to the screen.

3. Details for the whole process is:

	Parser --> Parser Anayslis --> execute subroutine

4. For selection process, a bunch algorithms are used for processing, such as natural join, cross join, one pass, two pass, nested loop algorithms

	```java
public class Join {
	public ArrayList<Tuple> NaturalJoin2Tables(ArrayList<String> tables,ArrayList<String> fieldkeys) {
		ArrayList<UTuple> tuples = new ArrayList<UTuple>();
		//two pass to sort tables first
		ArrayList<ArrayList<Integer>> records = new ArrayList<ArrayList<Integer>>();
		int originaltablesize = tables.size();
		String temp = "temp";
		ArrayList<String> fieldnames = new ArrayList<String>();
		....
	}
	
		public ArrayList<Tuple> CrossJoin2Tables(ArrayList<String> tables) {
		/*This is to join 2 tables together
		 */
		assert(tables.size() == 2);
		sortable(tables);
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		String table1 = tables.get(0);
		String table2 = tables.get(1);
		....
	}
	}
	```

	Then for the large relations, we have to use one-pass or two-pass algorithms such as:
	
	```java
	public class TwoPass {
		public static ArrayList<Integer> FirstPass(Relation relation_reference, MainMemory mem, Relation relation_referencetemp, String fieldkey) 
	{
		Heap<UTuple> heap = new Heap<UTuple>();
		ArrayList<Integer> record = new ArrayList<Integer>();
		int relationnum = relation_reference.getNumOfBlocks();
		int senttomem = mem.getMemorySize()>relationnum?relationnum:mem.getMemorySize();
		int alreadyinmem = 0;
		....
	}
	
	public ArrayList<UTuple> NaturalJoinSubList(ArrayList<String> tables, ArrayList<ArrayList<Integer>> records, ArrayList<String> fieldkeys)
	{
		RelationHelper.clearMainMemory(mem);
		ArrayList<UTuple> utuples = new ArrayList<UTuple>();
		Heap<UTuple> heap1 = new Heap<UTuple>();
		Heap<UTuple> heap2 = new Heap<UTuple>();
		....
	}
	```
    
5. Expression tree are also used to check the select condition requirement

	```java
	public class ExpressionTree {
	private String op;
	private ExpressionTree left, right;
	public ExpressionTree(){}
	public ExpressionTree(String str) { op = str;}
	public ExpressionTree(String str, ExpressionTree left, ExpressionTree right) 
	{	
		op = str;
		this.left = left;
		this.right = right;
	}
	```
The details are in the source code..