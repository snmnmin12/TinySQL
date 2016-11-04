package tinySQL;

import storageManager.*;

import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class Parser2{
	private final String error1 = "Syntax Error!";
	private final String error2 = "Create Error!";
	public String sentence;
	public ArrayList<String> words;
	public ArrayList<String> fields;
	public ArrayList<FieldType> fieldtypes;
	public ArrayList<Field>  values;
	public TreeNode select;
	public TreeNode delete;

	public Parser2() {
		words   = new ArrayList<String>();
		fields  = new ArrayList<String>();
		fieldtypes = new ArrayList<FieldType>();
		values = new ArrayList<Field>();
		select = null;
		delete = null;
	}

	public boolean SyntaxParse(String str) {
		if (str.length() == 0)
			return false;
		sentence = str.trim();
		reset();
		int i = 0;
		if (isLetter(sentence,i)) {
			Pair<String, Integer> res = lettersRetrieve(sentence, i);
			words.add(res.first);
			if ("create".equalsIgnoreCase(res.first))
				return CreateCommand(sentence, res.second);
			else if ("drop".equalsIgnoreCase(res.first))
				return DropCommand(sentence, res.second);
			else if ("insert".equalsIgnoreCase(res.first))
				return InsertCommand(sentence, res.second);
			else if("select".equalsIgnoreCase(res.first))
				return selectCommand(sentence, res.second);
			else if ("delete".equalsIgnoreCase(res.first))
				return deleteCommand(sentence, res.second);
			else if ("source".equalsIgnoreCase(res.first))
				return Letters(sentence, i);
			else {
				error(error1);
//				return false;
			}
				
		}else {
			error(error1);
		}
		return true;
	}

	public  boolean CreateCommand(String sentence, int i) {
		boolean flag = Letters(sentence, i);
		if (flag && !"table".equals(words.get(1).toLowerCase()))
			flag = false;
		return flag;
	}
	
	public boolean DropCommand(String sentence, int i) {
		boolean flag = Letters(sentence, i);
		if (flag && !"table".equals(words.get(1).toLowerCase()))
			flag = false;
		return true;
	}
	
	public boolean selectCommand(String sentence, int i) {
		select = new TreeNode("select");
		Pair<String, Integer> res;
		
		for (; i < sentence.length();) {
			i = spaceTrim(sentence,i);
			if (isLetter(sentence, i)) {
				res = lettersRetrieve(sentence, i);
				if ("distinct".equals(res.first.toLowerCase()))
					select.distinct = true;
				else if ("from".equals(res.first.toLowerCase())) {
					select.from  = true;
					String str;
//					res = lettersRetrieve(sentence, res.second+1);
					int endindex = sentence.toLowerCase().indexOf("where");
					if (endindex == -1) endindex = sentence.toLowerCase().indexOf("order");
					if (endindex == -1) endindex = sentence.length();
					str = sentence.substring(res.second,endindex).trim();
					select.table = str.split(",");
					i = endindex;
					continue;
				}
				else if ("where".equals(res.first.toLowerCase())) {
					select.where = true;
					spaceTrim(sentence, i);
				//check if order by exists or not,
					int index = sentence.toLowerCase().indexOf("order");
					String conditions = sentence.substring(res.second+1, index==-1?sentence.length():index).toLowerCase();
					select.conditions = ExpressionTree.BuildTree(conditions);
					if (index != -1) {
						index = sentence.toLowerCase().indexOf("by");
						if (index == -1) error("Error in order");
						String attri = sentence.substring(index+"by".length()).toLowerCase();
						if ("".equals(attri)) error("Error in order, No Attributes Given");
						select.order_by = attri.trim();
					}
					break;
				} else if("order".equalsIgnoreCase(res.first)) {
					int index = sentence.toLowerCase().indexOf("by");
					if(index == -1) error("Error in order");
					String order_by = sentence.substring(index+"by".length()).toLowerCase().trim();
					select.order_by = order_by;
					break;
				}
				else {
					select.attributes.add(res.first);
				}
				i = res.second;
			}
			else if (isSpace(sentence, i) || sentence.charAt(i) == ',') i++;
			else if (sentence.charAt(i) == '*'){
				select.attributes.add("*");
				i++;
			}
		}
		return true;
	}
	
	public boolean deleteCommand(String sentence, int i) {
		delete = new TreeNode("delete");
		Pair<String, Integer> res;
		for (; i < sentence.length();) {
			spaceTrim(sentence,i);
			if (isLetter(sentence, i)) {
			res = lettersRetrieve(sentence, i);
				if ("from".equalsIgnoreCase(res.first)) {
					delete.from  = true;
					res = lettersRetrieve(sentence, res.second);
					delete.table =new String[]{res.first};
				}
				else if ("where".equalsIgnoreCase(res.first)) {
					delete.where = true;
					String e = sentence.substring(res.second+1, sentence.length()).trim();
					delete.conditions = ExpressionTree.BuildTree(e);
					break;
				}else {
					return false;
				}
				i = res.second;
			}
			else if (isSpace(sentence, i)) i++;
		}
		return true;
	}
	
	public boolean InsertCommand(String sentence, int i) {
		boolean flag = Letters(sentence, i);
		if (flag && !"into".equals(words.get(1).toLowerCase()))
			flag = false;
		return true;
	}
	
	// helper method to check letter or display error messages

	public static void error(String message) {
		System.out.println(message);
		System.exit(0);
	}

	//trim all spaces
	private int spaceTrim(String sentence, int i) {
		if(!isSpace(sentence, i)) return i;
		while(isSpace(sentence, i)) i++;
		return i;
	}

	//helper method to retrive all the letters from the string
	public boolean Letters(String sentence, int i) {
		Pair<String, Integer> res;
		for (; i < sentence.length();) {
			i = spaceTrim(sentence,i);
			if (isLetter(sentence, i)) {
			res = lettersRetrieve(sentence, i);
			words.add(res.first);
			if ("select".equalsIgnoreCase(res.first))
				return selectCommand(sentence, res.second);
			i = res.second;
			}else if (sentence.charAt(i) == '('){
			i = attributeRetrieve(sentence, i);
			}
		}
		return true;
	}
	
	private Pair<String, Integer> lettersRetrieve(String sentence, int i){
		i = spaceTrim(sentence, i);
		if(!isLetter(sentence, i)) return null;
		StringBuilder word = new StringBuilder();
		do{
			word.append(sentence.charAt(i++));
		}while(isAlanum(sentence, i) || isDot(sentence, i));
		return new Pair<String, Integer>(word.toString(), i);
	}
	
	//retrive all attributes or values 
	private int attributeRetrieve(String sentence, int i) {
		//to increate i to bypass(
		int rightp = sentence.indexOf(')',i);
		if (rightp == -1) 
			error("Parenthesis Error!");
		String sub = sentence.substring(i+1, rightp).trim();
		String[] subs = sub.split(",");
		boolean valueflag = false;
		if (words.size() > 0 && "values".equalsIgnoreCase(words.get(words.size()-1)))
			valueflag = true;
		
		for (String str:subs) {
			Field f = new Field();
			String[] subsub = str.trim().split(" ");
			if (isDigit(subsub[0],0)) {
				f.type = FieldType.INT;
				f.integer = Integer.valueOf(subsub[0]);
				values.add(f);
			}else {
				f.type = FieldType.STR20;
				f.str = subsub[0];
				if(valueflag) values.add(f);
				else fields.add(f.str);
				if(subsub.length == 2) {
					if (subsub[1].equalsIgnoreCase("int"))
						fieldtypes.add(FieldType.INT);
					else if (subsub[1].equalsIgnoreCase("str20"))
						fieldtypes.add(FieldType.STR20);
					else error("Insertion Wrong!");
				}
				}
		}
		return rightp+1;
	}
	
	// helper method to check letter
	private boolean isDot(String sentence, int i) {
		return safeCheck(i) && sentence.charAt(i) == '.';
	}
	private boolean isSpace(String sentence, int i) {
		return Character.isSpaceChar(sentence.charAt(i));
	}

	private boolean isLetter(String sentence, int i) {
		return safeCheck(i) && Character.isLetter(sentence.charAt(i));
	}

	private boolean isDigit(String sentence, int i) {
		return safeCheck(i) && Character.isDigit(sentence.charAt(i));
	}

	private boolean isAlanum(String sentence, int i) {
		return safeCheck(i) && (isDigit(sentence,i)|| isLetter(sentence,i));
	}
	//check index within the range
	private boolean safeCheck(int i) {
		return i < sentence.length();
	}
	public void reset() {
		words.clear();
		fields.clear();
		fieldtypes.clear();
		values.clear();
		select = null;
		delete = null;
	}
	
	public String toString() {
		String str = "";
		str += sentence+"\n";
		if (words.size() !=0 )
			str +="Statements: "+ words+"\n";
		if (fields.size() != 0)
			str +="Attributes: "+ fields+"\n";
		if (fieldtypes.size() !=0 )
			str += "FieldTypes: "+ fieldtypes+"\n";
		if (values.size() !=0 )
			str += "Values: "+ values+"\n";
		if (select != null)
			str += select;
		if (delete != null)
			str += delete;
		return str;
	}
	
	public static void parseFile(String... files) throws IOException
	{
	    if(files.length == 0) error("Error files");
	    Parser2 parse = new Parser2();
	    List<String> lines = new ArrayList<String>();
	    File file = new File(files[0]); //for ex foo.txt
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (parse.SyntaxParse(line)) {
		    		System.out.println(parse);
					//lines.add(parse.toString());
				}
		    }
		}catch(IOException e) {
			System.out.println(e);
		}
		if (files.length == 1) {
			for (String str:lines) {
				System.out.println(str);
			}
		}else {
			PrintWriter writer = new PrintWriter(files[1]);
			for (String str:lines)
				writer.println(str);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		Parser2 parse = new Parser2();
		String filename = "test2.txt";
		String filename2 = "output.txt";
		String input = "source test.txt";
		//System.out.println(input.indexOf(" "));
		//File file = new File(filename);
//		String sample = "SELECT * FROM course WHERE exam = 100 ORDER BY exam";
//		System.out.println(sample.toLowerCase());
//		System.out.println(sample);
//		sample.m
		try {
		 parseFile(filename);
		}catch(IOException e){
			System.out.println(e);
		}
		//parseFile(filename);
//		String statement = "SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam";
//		if (parse.SyntaxParse(statement)) {
//		System.out.println(parse);
//		}
//		String statement = "INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course";
//		if (parse.SyntaxParse(statement)) {
//			System.out.println(parse);
//		}
//		if (parse.SyntaxParse("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)")) {
//			System.out.println(parse);
//		}
//		if (parse.SyntaxParse("INSERT INTO course (sid, homework, project, exam, grade)")) {
//		System.out.println(parse);
//		}
//		if(parse.SyntaxParse("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")"))
//				System.out.println(parse);
//		if (parse.SyntaxParse("DELETE FROM course WHERE grade = \"E\"")) {
//			System.out.println(parse);
//			}
//		if (parse.SyntaxParse("SELECT * FROM course WHERE exam = 100 ORDER BY exam")) {
//		System.out.println(parse);
//		}
//		if (parse.SyntaxParse("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, 'A')")) {
//			System.out.println(parse);
//		}
//		if (parse.SyntaxParse("SELECT * FROM course")) {
//			System.out.println(parse.words());
//			System.out.println(parse.fields);
//			System.out.println(parse.fieldtypes);
//			System.out.println(parse.values);
//		}
	}
}

