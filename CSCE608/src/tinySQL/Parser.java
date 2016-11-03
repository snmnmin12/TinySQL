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

public class Parser{
	private final String error1 = "Syntax Error!";
	private final String error2 = "Create Error!";
	private static final Set<String> statements =
        new HashSet<String>(Arrays.asList("create","drop","select","delete","insert","source"));
	private String sentence;
	private ArrayList<String> words;
	private ArrayList<String> fields;
	private ArrayList<String> fieldtypes;
	private ArrayList<Field> values;
	private boolean allAttributeds;

	public Parser() {
		words   = new ArrayList<String>();
		fields  = new ArrayList<String>();
		fieldtypes = new ArrayList<String>();
		values = new ArrayList<Field>();
		allAttributeds = false;
	}

	public boolean SyntaxParse(String str) {
		if (str.length() == 0)
			return false;
		sentence = str;
		reset();
		//sentence = str.toLowerCase();
		//control the start is number or not
		for (int i = 0; i < str.length();) {

			if (!keyWordCheck()) {
				error(error1);
				return false;
			}

			if (isAlanum(sentence,i)) {
				Pair<String, Integer> res = lettersRetrieve(sentence, i);
				words.add(res.first);
				i = res.second;
			} 

			else if (isSpace(sentence, i)) {
				i = spaceTrim(sentence, i);
			} 
			else if (sentence.charAt(i) == '*') {
				allAttributeds = true;
				i++;
			}else if (sentence.charAt(i) == '(') {
				i = attributeRetrieve(sentence, i);
			}
			else return false;
		}
		return true;
	}

	public ArrayList<String> words() {
		return words;
	}
	
	public ArrayList<String> fields() {
		return fields;
	}
	
	public ArrayList<String> fieldtypes() {
		return fieldtypes;
	}
	
	public ArrayList<Field> values() {
		return values;
	}
	
//	public String toString() {
//		return sentence;
//	}
	// helper method to check letter or display error messages

	private void error(String message) {
		System.out.println(message);
	}

	private boolean keyWordCheck() {
		if (words.size() > 0) 
			if (!statements.contains(words.get(0).toLowerCase())) {
				words.clear();
				return false;
			}
		return true;
	}

	//check the keyword is create or not
	private boolean isCreate() {
		return words.get(0).equals("create");
	}

	//trim all spaces
	private int spaceTrim(String sentence, int i) {
		while(isSpace(sentence, ++i));
		return i;
	}

	//helper method to retrive all the letters from the string
	private Pair<String, Integer> lettersRetrieve(String sentence, int i){
		if(!isLetter(sentence, i)) 
			return null;
		StringBuilder word = new StringBuilder();
		do{
			word.append(sentence.charAt(i++));
		}while( isAlanum(sentence, i) || isDot(sentence, i));
		return new Pair<String, Integer>(word.toString(), i);
	}
	
	//retrive all attributes or values 
	private int attributeRetrieve(String sentence, int i) {
		//to increate i to bypass(
		i++;
		boolean errorflag = false;
		boolean valueflag = false;
		if (words.size() > 0 && words.get(words.size()-1).toLowerCase().equals("values"))
			valueflag = true;
		do{
			while(sentence.charAt(i) != ',' && sentence.charAt(i) != ')') {
			Field f = new Field();
			if (isDigit(sentence, i)) {
				Pair<Integer, Integer> nums =  numbersRetrieve(sentence, i);
				f.integer = nums.first;
				if (valueflag) values.add(f);
			    i = nums.second;
			}else if (isLetter(sentence, i)) {
				Pair<String, Integer> letters =  lettersRetrieve(sentence, i);
				f.type = FieldType.STR20;
				f.str = letters.first;
				if (valueflag) 
					values.add(f);
				else {
					fields.add(letters.first);
					i = letters.second;
					letters =  lettersRetrieve(sentence, i+1);
					if (letters != null && !letters.first.toLowerCase().equals("int") && !letters.first.toLowerCase().equals("str20")) {
						errorflag = true;
						break;
					}
					if (letters != null)
						fieldtypes.add(letters.first);
				}
			    if (letters != null)
			    	i = letters.second;
			}else
				i++;
			}
			if (sentence.charAt(i) == ',') {
//				System.out.println(sentence.charAt(i));
				i++;
			}
		}while(sentence.charAt(i) != ')');
		//to bypass the right ')';
		i++;
		return i;
	}

	private Pair<Integer, Integer> numbersRetrieve(String sentence, int i) {
		if(!isDigit(sentence, i))
			return null;
		StringBuilder number = new StringBuilder();
		do {
		number.append(sentence.charAt(i++));
		}while(isDigit(sentence, i));
		return new Pair<Integer, Integer>(Integer.valueOf(number.toString()), i);
	}

//	public void create(table name, fields, fieldtype) {
//		schema(fields, fieldtype);
//		schema_manager.createRelation(relation_name,schema);
//		return 
//	}
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
		allAttributeds = false;
	}
	
	public String toString() {
		String str = "";
		str += sentence+"\n";
		str +="Statements: "+ words+"\n";
		str +="Attributes: "+ fields+"\n";
		str += "FieldTypes: "+ fieldtypes+"\n";
		str += "Values: "+ values+"\n\n";
		return str;
	}
	
	public static void parseFile(String... files) throws IOException
	{
	    if(files.length == 0) {
	    	System.out.println("Error files");
	    	return;
	    }
	    Parser parse = new Parser();
	    List<String> lines = new ArrayList<String>();
	    File file = new File(files[0]); //for ex foo.txt
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (parse.SyntaxParse(line)) {
					lines.add(parse.toString());
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
		
		Parser parse = new Parser();
		String filename = "test.txt";
		String filename2 = "output.txt";
		String input = "source test.txt output.txt";
		//File file = new File(filename);
//		try {
//		String content = readFile(filename);
//		System.out.println(content);
//		}catch(IOException e){
//			System.out.println(e);
//		}
//		parseFile(filename,filename2);
//		if (parse.SyntaxParse("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)")) {
//			System.out.println(parse.words());
//			System.out.println(parse.fields);
//			System.out.println(parse.fieldtypes);
//			System.out.println(parse.values);
////			System.out.println(parse.numbers());
//		}
		if (parse.SyntaxParse(input)) {
			System.out.println(parse);
		}		
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
