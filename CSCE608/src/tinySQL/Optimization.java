package tinySQL;

import java.util.ArrayList;

import storageManager.*;

public class Optimization {
	
	public static void JoinOptimization(ArrayList<String> tables, SchemaManager schem_manager) {
		Relation relation_referene = schem_manager.getRelation(tables.get(0));
		int size = tables.size();
		
	}
	
	public static void SelectOptimization() {
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
