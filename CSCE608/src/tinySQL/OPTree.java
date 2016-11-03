package tinySQL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Schema;
import storageManager.Tuple;

public class OPTree {
	private String op;
	private OPTree left, right;
	public OPTree(){}
	public OPTree(String str) {
		op = str;
	}
	
	private static Pair<String, Integer> words(String str, int i) {
		
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
	
	private static boolean isAlnum(String str, int i) {
		return Character.isAlphabetic(str.charAt(i)) || Character.isDigit(str.charAt(i));
	}
	
	public static OPTree BuildTree(String str) {
		
		Queue<String> que = new LinkedList<String>();
		Stack<String> stack = new Stack<String>();
		OPTree root = null;
		int pa = 0;
		
		str = str.trim();
		
		ArrayList<String> sta = new ArrayList<String>();
		Pair<String, Integer> res;
		
		for (int i = 0; i < str.length();) {
		  res = words(str, i);
		  if ("and".equalsIgnoreCase(res.first) ||"or".equalsIgnoreCase(res.first)) {
			  if ("and".equalsIgnoreCase(res.first)) res.first = "&";
			  if ("or".equalsIgnoreCase(res.first)) res.first = "|";
			  if (root != null) {
					  OPTree node = expression(que);
					  if (root.right == null) 
						  root.right = node;
					  else {
						  node.left = root;
						  root = node;
					  }
				  que.clear();
			  }
			  else {
//				  root = new OTree("|");
				  root = expression(que);
//				  if ("and".equalsIgnoreCase(res.first) )
//					  root.op = "&";
				  que.clear();
			  }
		  }else if("(".equals(res.first)) {
				if (que.size()!=0) {
					root = expression(que);
					que.clear();
				}
				pa++;
		  }

		  if (pa > 0) stack.add(res.first);
		  if (")".equals(res.first)) {
//				stack.add(res.first);
				OPTree node = expression(stack);
				if (root !=null)
					root.right = node;
				else root = node;
				pa--;
			}
		  i = res.second;
		  if(pa == 0 && !"(".equals(res.first) && !"[".equals(res.first) && !")".equals(res.first)&& !"]".equals(res.first))
			  que.add(res.first);
		}
		
		//check if the parentheses are matched;
		if (pa != 0 ) {
			System.out.println("Parenthisis Error");
			System.exit(0);
		}
		//final processing the node
		if (que.size() !=0 ) {
			OPTree node = null;
			node = expression(que);
			if (root != null)  {
				if (root.right == null)
					node.right = root;
				else {
				node.left = root;
				root = node;
				}
			}else root = node;
		}
		return root;
	}
	
	private  static OPTree expression(Stack<String> sta) {
		if (sta.size() == 0) 
			return null;
		OPTree tree = null;
		String s = sta.pop();
		if (isAlnum(s,0)) {
			tree = expression(sta);
			if(tree == null) 
				tree = new OPTree(s);
			else 
				tree.right = new OPTree(s);
		}else if (!")".equals(s) && !"(".equals(s) && !"]".equals(s) && !"[".equals(s)) {
			tree = new OPTree(s);
			tree.left = expression(sta);
		}else {
			tree = expression(sta);
		}
		return tree;
	}
	
	private static OPTree expression(Queue<String> sta) {
		if (sta.size() == 0) 
			return null;
		OPTree tree = null;
		String s = sta.poll();
		if (isAlnum(s,0)) {
			tree = expression(sta);
			if(tree == null) 
				tree = new OPTree(s);
			else 
				tree.left = new OPTree(s);
		}else {
			tree = new OPTree(s);
			tree.right = expression(sta);
		}
		return tree;
	}
	
	public String toString() {
		return preorder();
	}
	
	private String preorder() {
		String str = "";
		if (left != null) 
			str += "("+left.preorder();
		str += op;
		if (right != null)
			str += right.preorder()+")";
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
//		String words = "course.sid = course2.sid AND course.exam > course2.exam";
//		String words2 = "course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100";
//		String words3 = "course.sid = course2.sid AND course.grade = \"A\" AND course2.grade = \"A\"";
//		String words4 = "(exam + homework) = 200";
//		String words5 = "r.a = t.a AND r.b = s.b AND s.c = t.c";
//		
//		OPTree otree = BuildTree(words3);
//		System.out.println(otree.preorder());
	}

}
