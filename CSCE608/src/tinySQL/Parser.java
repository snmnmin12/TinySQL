package tinySQL;

import storageManager.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class Parser{
	private final String error1 = "Syntax Error!";
	private static final Set<String> statements =
        new HashSet<String>(Arrays.asList("create","drop","select","delete","insert"));
	private String sentence;
	private ArrayList<String> words;
	private ArrayList<String> fields;
	private ArrayList<FieldType> fieldtypes;
	private ArrayList<Field> values;
	private boolean allAttributeds;

	public Parser() {
		words   = new ArrayList<String>();
		allAttributeds = false;
	}

	public boolean SyntaxParse(String str) {
		
		if (str.length() == 0)
			return false;
		
		sentence = str.toLowerCase();
		int i = 0;
		if (isAlanum(sentence, 0)) {
			i = lettersRetrieve(sentence,0);
			if 
		}
		//control the start is number or not
//		for (int i = 0; i < str.length();) {
//
//			if (!keyWordCheck()) {
//				error(error1);
//				return false;
//			}
//
//			if (isAlanum(sentence,i)) {
//				i = lettersRetrieve(sentence, i);
//			} 
//			else if (isDigit(sentence, i)) {
//				i = numbersRetrieve(sentence, i);
//			} 
//			else if (isSpace(sentence, i)) {
//				i = spaceTrim(sentence, i);
//			} 
//			else if (sentence.charAt(i) == '*') {
//				allAttributeds = true;
//				i++;
//			}
//			else return false;
//		}
		return true;
	}

	public ArrayList<String> words() {
		return words;
	}

//	public ArrayList<Integer> numbers() {
//		return numbers;
//	}

	public String toString() {
		return sentence;
	}
	// helper method to check letter or display error messages

	private void error(String message) {
		System.out.println(message);
	}

	private boolean keyWordCheck() {
		if (words.size() > 0) 
			if (!statements.contains(words.get(0))) {
				words.clear();
//				numbers.clear();
				return false;
			}
		return true;
	}

	//check the keyword is create or not
	private boolean isCreate() {
		return words.get(0).equals("create");
	}

	//check index within the range
	private boolean safeCheck(int i) {
		return i < sentence.length();
	}
	//trim all spaces
	private int spaceTrim(String sentence, int i) {
		while(isSpace(sentence, ++i));
		return i;
	}

	//helper method to retrive all the letters from the string
	private int lettersRetrieve(String sentence, int i){
		StringBuilder word = new StringBuilder();
		do{
			word.append(sentence.charAt(i++));
		}while(isAlanum(sentence, i));
		words.add(word.toString());
		word.setLength(0);
		return i;
	}

	private int numbersRetrieve(String sentence, int i) {
		StringBuilder number = new StringBuilder();
		do {
		number.append(sentence.charAt(i++));
		}while(isDigit(sentence, i));
		numbers.add(Integer.valueOf(number.toString()));
		number.setLength(0);
		return i;
	}

	// helper method to check letter
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
	
	public static void main(String[] args) {
		Parser parse = new Parser();
		if (parse.SyntaxParse("CREATE TABLE course12")) {
			System.out.println(parse.words());
//			System.out.println(parse.numbers());
		}
	}
}
