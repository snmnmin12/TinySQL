package tinySQL;

import java.util.ArrayList;

public class TreeNode {
	public String name;
	public boolean distinct;
	public ArrayList<String> attributes;
	public boolean from;
	public String[] table;
	public boolean where;
	public ExpressionTree conditions;
	public TreeNode parent;
	public TreeNode child;
	public String order_by;
	public TreeNode(String name) {
		attributes = new ArrayList<String>();
		this.name = name;
	}
	public String toString() {
		String str = "";
		str += name+" ";
		if (distinct) str += "distinct ";
		if (attributes.size()!=0)
			str += String.join(",", attributes)+" ";
		if (from) str += "from ";
		str += String.join(",",table) +" ";
		if (where) {
			str += "where ";
			str += conditions+" ";
		}
		if (order_by != null)
			str += "order by " + order_by;
		str += "\n";
		return str;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
