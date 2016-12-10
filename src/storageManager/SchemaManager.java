package storageManager;

import java.io.Serializable;
import java.util.TreeMap;

/* A schema manager maps a relation name to a relation and 
 * 	a corresponding schema. 
 * You will always create a relation through schema manager 
 * by specifying a relation name and a schema.
 * You will also get access to relations and schemas from here.
 * Usage: At the beginning of your program, you need to initialize a 
 * 			schema manager.
 *        Initialize the schema manager by supplying the references 
 *        	to memory and to disk
 *        Create a relation through here (and not elsewhere) by giving 
 *        	relation name and schema
 *        Every relation name must be unique.
 *        Once a relation is created, the schema cannot be changed
 */

public class SchemaManager implements Serializable {
	  public final static int MAX_NUM_CREATING_RELATIONS = 100;
	  private MainMemory mem;
	  private Disk disk;
	  private TreeMap<String,Integer> relation_name_to_index;
	  private Relation[] relations
	  						=new Relation[MAX_NUM_CREATING_RELATIONS];
	  protected Schema[] schemas
	  						=new Schema[MAX_NUM_CREATING_RELATIONS];
	  private int offset;

	  public SchemaManager(MainMemory mem, Disk disk) {
		    this.mem=mem;
		    this.disk=disk;
		    offset=0;
		    for (int i=0;i<MAX_NUM_CREATING_RELATIONS;i++) {
		    	relations[i]=new Relation();
		    	schemas[i]=new Schema();
		    }
		    relation_name_to_index=new TreeMap<String,Integer>();
	  }

	//returns empty schema if the relation is not found
	  public Schema getSchema(String relation_name)  {
	    if (!relation_name_to_index.containsKey(relation_name)) {
	      System.err.print("getSchema ERROR: relation " + 
	    		  relation_name + " does not exist" + "\n");
	      return new Schema();
	    } else {
	      return new Schema(schemas[
	    		  relation_name_to_index.get(relation_name)]);
	    }
	  }

	//returns true if the relation exists
	  public boolean relationExists(String relation_name)  {
	    return (relation_name_to_index.containsKey(relation_name));
	  }

	// returns a reference to the newly allocated relation; 
	// the relation name must not exist already
	  public Relation createRelation(String relation_name, 
			  Schema schema){
	    if (relation_name=="") {
	      System.err.print("createRelation ERROR: empty relation name"
	    		  + "\n");
	      return null;
	    }
	    if (relation_name_to_index.containsKey(relation_name)) {
	      System.err.print("createRelation ERROR: " + relation_name 
	    		  + " already exists" + "\n");
	      return null;
	    }
	    if (schema.isEmpty()) {
	      System.err.print("createRelation ERROR: empty schema" 
	    		  + "\n");
	      return null;
	    }
	    if (offset==MAX_NUM_CREATING_RELATIONS) {
	      System.err.print("createRelation ERROR: no more " +
	      		"relations can be created." + "\n");
	      return null;
	    }
	    relation_name_to_index.put(relation_name,offset);
	    relations[offset]=new Relation(this,offset,relation_name,
	    		mem,disk);
	    schemas[offset]=new Schema(schema);
	    offset++; // increase the boundary
	    return relations[offset-1];
	  }

	//returns null if the relation is not found
	  public Relation getRelation(String relation_name) {
	    if (!relation_name_to_index.containsKey(relation_name)) {
	      System.err.print("getRelation ERROR: relation " 
	    		  + relation_name + " does not exist" + "\n");
	      return null;
	    } else {
	      return relations[
	    		  relation_name_to_index.get(relation_name)];
	    }
	  }

	//returns false if the relation is not found
	  public boolean deleteRelation(String relation_name) {
	    if (!relation_name_to_index.containsKey(relation_name)) {
	      System.err.print("deleteRelation ERROR: relation " 
	    		  + relation_name + " does not exist" + "\n");
	      return false;
	    }
	    int offset=relation_name_to_index.get(relation_name);
	    relations[offset].invalidate();
	    schemas[offset].clear();
	    relation_name_to_index.remove(relation_name);
	    return true;
	  }

	  public String toString()  {
	    String str="";
	    if (offset>0) {
	      int i;

	      for (i=0;i<offset;i++) {
	        if (!relations[i].isNull()) {
	          str+=(relations[i].getRelationName() + "\n");
	          str+=schemas[i].toString();
	          break;
	        }
	      }
	      for (i++;i<offset;i++) {
	        if (!relations[i].isNull()) {
	          str+=("\n");
	          str+=(relations[i].getRelationName() + "\n");
	          str+=schemas[i].toString();
	        }
	      }
	    }
	    return str;
	  }
}
