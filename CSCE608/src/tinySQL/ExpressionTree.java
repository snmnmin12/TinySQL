package tinySQL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Schema;
import storageManager.Tuple;
/*
 DELETE FROM course WHERE grade = "E"
SELECT * FROM course WHERE exam = 100
SELECT * FROM course WHERE grade = "A"
SELECT * FROM course WHERE exam = 100 AND project = 100
SELECT * FROM course WHERE exam = 100 OR exam = 99
SELECT * FROM course WHERE exam > 70
SELECT * FROM course WHERE exam = 100 OR homework = 100 AND project = 100
SELECT * FROM course WHERE (exam + homework) = 200 
SELECT course.sid, course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam
SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100
SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam
SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam AND course.homework = 100
SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND course.grade = "A" AND course2.grade = "A" ORDER BY course.exam
SELECT * FROM r, s, t WHERE r.a = t.a AND r.b = s.b AND s.c = t.c
 */


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
	
	public static ExpressionTree BuildTree(String str) {
		TreeBuild bt = new TreeBuild(str.trim());
		return bt.treeroot;
	}
	
	
	private static class TreeBuild {
		public ExpressionTree treeroot;
		private Stack<String> operator;
		private Stack<ExpressionTree> operand;
			public TreeBuild(String str) {
			operator = new Stack<String>();
			operand = new Stack<ExpressionTree>();
			treeroot = BuildT(str);
		}
		
		private Pair<String, Integer> words(String str, int i) {
				
				StringBuilder sb = new StringBuilder();
				//trim space
				while(i < str.length() && Character.isSpaceChar(str.charAt(i))) i++;
				//after trimming check parenseses
				if (str.charAt(i) != '"' && !isAlnum(str, i) ) {
					sb.append(str.charAt(i++));
					return new Pair<String,Integer>(sb.toString(), i);
				}
				//retrieve letters
				while(i < str.length() && (isAlnum(str,i) || str.charAt(i) == '.' || str.charAt(i) == '"') ) 
				{
					sb.append(str.charAt(i++));
				}
				return new Pair<String,Integer>(sb.toString(), i);
			}
			
		private boolean isAlnum(String str, int i) {
				return Character.isAlphabetic(str.charAt(i)) || Character.isDigit(str.charAt(i));
			}
		private ArrayList<String> letters(String str) {
			Pair<String, Integer> res;
			ArrayList<String> words = new ArrayList<String>();
			for (int i = 0; i < str.length();) {
				  res = words(str, i);
				  if (res.first.equalsIgnoreCase("and"))
					  words.add("&&");
				  else if (res.first.equalsIgnoreCase("or"))
					  words.add("||");
				  else
					  words.add(res.first.trim());
				  i = res.second;
			}
			return words;
		}
		//construct expression tree according to grammars, including parenthesis, +,-,*,/
		private ExpressionTree BuildT(String str) {
			ExpressionTree root = new ExpressionTree();
			ArrayList<String> words = letters(str);
			for (int i = 0; i < words.size(); i++) 
			{
				char ch = words.get(i).charAt(0);
				switch(ch) {
					case '+':
					case '-':
					case '*':
					case '/':
					case '&':
					case '|':
					case '=':
					case '>':
					case '<':
						precedenceProcess(words.get(i));
						break;
	                case ')':
                        processRightParenthesis();
                        break;
	                case '(':
                        operator.push(words.get(i));
                        break;
                    default:
                    	operand.add(new ExpressionTree(words.get(i)));
                    	continue;
				}
			}
		    while (!operator.empty()) {
		        operation(operator.pop());
		    }
		    // Invariant: At this point the operandStack should have only one element
		    //     operandStack.size() == 1
		    // otherwise, the expression is not well formed.
		    if ( operand.size()  != 1) {
		    	Parser2.error("Not well formed tree!");
		    }
		    
		    root  = operand.pop();
			return root;
		}
		private int precedence(char op)
		{
		    switch (op) {
		        case '+':
		        case '-':
		        case '=':
		        case '>':
		        case '<':
		            return 1;
		        case '/':
		        case '*':
		            return 2;
		        default:
		            return 0;
		    }
		}
		private void precedenceProcess(String ch) {
			int op = precedence(ch.charAt(0));
		    while ((!operator.empty()) && (op <= precedence(operator.peek().charAt(0))))   {
		        operation(operator.pop());
		    }
		    // lastly push the operator passed onto the operatorStack
		    operator.push(ch);
		}
        private void processRightParenthesis() {
            while (!operator.empty() && !operator.peek().equals("(")) {
                operation(operator.pop());
            }
            operator.pop(); // remove '('
        }
		// takes their place on the top of the stack.
		private void operation(String op)
		{
		    ExpressionTree right = operand.pop();
		    ExpressionTree left = operand.pop();
		    ExpressionTree p= new ExpressionTree(op, left, right);
		    operand.push(p);
		}
	}
	
	public String toString() {
		return inorder();
	}
	
	private String inorder() {
		String str = "";
		if (left != null) 
			str += "("+left.inorder();
		str += op;
		if (right != null)
			str += right.inorder()+")";
		return str;
	}
	
	public boolean check(Schema schema, Tuple tuple) {
		String str = evaluate(schema, tuple);
		if (str.equals("false")) return false;
		if (!str.equals("true")) Parser2.error("Syntax Error!");
		return true;
	}
	
	public int contains(ArrayList<String> arr, String e) {
		int i = e.indexOf('.');
		if (i != -1) e = e.substring(i+1);
		for (i = 0; i < arr.size(); i++)
			if (arr.get(i).equalsIgnoreCase(e)) 
				return i;
		return -1;
	}
	
	public String evaluate(Schema schema, Tuple tuple) {
		if (left == null) { 
			int i = contains(schema.getFieldNames(),op);
			if (i == -1) return op;
			else {
				Field f = tuple.getField(i);
				if (f.type == FieldType.INT)
					return f.toString();
				else return f.str;
			}
		}
		//make operation below
		String lvalue = null;
		String rvalue = null;
		
		if (left != null) 
			lvalue = left.evaluate(schema, tuple);
		if (right != null)
			rvalue = right.evaluate(schema, tuple);
		
		String result = null;

		if (Character.isDigit(lvalue.charAt(0))) {
			boolean res = false;
			boolean flag = false;
			if (op.equals("=")) {
				res = (Integer.parseInt(lvalue) == Integer.parseInt(rvalue));
				flag = true;
			}else if(op.equals(">")) {
				res = (Integer.parseInt(lvalue) > Integer.parseInt(rvalue));
				flag = true;
			}
			else if (op.equals("<")) {
				res = (Integer.parseInt(lvalue) < Integer.parseInt(rvalue));
				flag = true;
			}
			else if(op.equals(">=")) {
				res = (Integer.parseInt(lvalue) >= Integer.parseInt(rvalue));
				flag = true;
			}
			else if (op.equals("<=")) {
				res = (Integer.parseInt(lvalue) <= Integer.parseInt(rvalue));
				flag = true;
			}
			if (flag) {
				if (!res) result = "false";
				else result = "true";
				return result;
			}
			if (op.equals("+")) {
				Integer sum = (Integer.parseInt(lvalue) + Integer.parseInt(rvalue));
				return sum.toString();
			}else if(op.equals("-")) {
				Integer sum = (Integer.parseInt(lvalue) - Integer.parseInt(rvalue));
				return sum.toString();
			}	
		}else if(Character.isLetter(lvalue.charAt(0)) || lvalue.charAt(0) == '"') {
			if (op.equals("=")) {
				boolean res = (lvalue.equalsIgnoreCase(rvalue));
				if (!res) return "false";
				else return "true";
			}else if(op.equals("&")) {
				if (lvalue.equals("false"))
				   return "false";
				if (rvalue.equals("false"))
					return "false";
				return "true";
			}else if(op.equals("|")) {
				if (lvalue.equals("true"))
					   return "true";
					if (rvalue.equals("true"))
						return "true";
					return "false";
				}
		}
		return result;
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String words = "course.sid = course2.sid AND course.exam > course2.exam";
		String words2 = "course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100";
		String words3 = "course.sid = course2.sid AND course.grade = \"A\" AND course2.grade = \"A\"";
		String words4 = "(exam + homework) = 200";
		String words5 = "r.a = t.a AND r.b = s.b AND s.c = t.c";
//		BuildT(words);
//		BuildT(words2);
//		BuildT(words3);
//		BuildT(words4);
//		BuildT(words5);
		ExpressionTree otree = BuildTree(words5);
		System.out.println(otree);
	}

}
