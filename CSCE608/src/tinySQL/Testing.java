package tinySQL;

import java.util.*;
import java.util.ArrayList;
import storageManager.*;

public class Testing {

  // An example procedure of appending a tuple to the end of a relation
  // using memory block "memory_block_index" as output buffer
  private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
    Block block_reference;
    if (relation_reference.getNumOfBlocks()==0) {
      System.out.print("The relation is empty" + "\n");
      System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
      block_reference=mem.getBlock(memory_block_index);
      block_reference.clear(); //clear the block
      block_reference.appendTuple(tuple); // append the tuple
      System.out.print("Write to the first block of the relation" + "\n");
      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
    } else {
      System.out.print("Read the last block of the relation into memory block 5:" + "\n");
      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
      block_reference=mem.getBlock(memory_block_index);

      if (block_reference.isFull()) {
        System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
        block_reference.clear(); //clear the block
        block_reference.appendTuple(tuple); // append the tuple
        System.out.print("Write to a new block at the end of the relation" + "\n");
        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
      } else {
        System.out.print("(The block is not full: Append it directly)" + "\n");
        block_reference.appendTuple(tuple); // append the tuple
        System.out.print("Write to the last block of the relation" + "\n");
        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
      }
    }
  }

  public static void main(String[] args){
    //=======================Initialization=========================
    System.out.print("=======================Initialization=========================" + "\n");

    // Initialize the memory, disk and the schema manager
    MainMemory mem=new MainMemory();
    Disk disk=new Disk();
    System.out.print("The memory contains " + mem.getMemorySize() + " blocks" + "\n");
    System.out.print(mem + "\n" + "\n");
    SchemaManager schema_manager=new SchemaManager(mem,disk);

    disk.resetDiskIOs();
    disk.resetDiskTimer();

    // Another way to time
    long start = System.currentTimeMillis(); 

    //=======================Schema=========================
    System.out.print("=======================Schema=========================" + "\n");

    // Create a schema
    System.out.print("Creating a schema" + "\n");
    ArrayList<String> field_names=new ArrayList<String>();
    ArrayList<FieldType> field_types=new ArrayList<FieldType>();
    field_names.add("f1");
    field_names.add("f2");
    field_names.add("f3");
    field_names.add("f4");
    field_types.add(FieldType.STR20);
    field_types.add(FieldType.STR20);
    field_types.add(FieldType.INT);
    field_types.add(FieldType.STR20);
    Schema schema=new Schema(field_names,field_types);

    // Print the information about the schema
    System.out.print(schema + "\n");
    System.out.print("The schema has " + schema.getNumOfFields() + " fields" + "\n");
    System.out.print("The schema allows " + schema.getTuplesPerBlock() + " tuples per block" + "\n");
    System.out.print("The schema has field names: " + "\n");
    field_names=schema.getFieldNames();
    System.out.print(field_names.toString()+"\n");
    System.out.print("The schema has field types: " + "\n");
    field_types=schema.getFieldTypes();
    System.out.print(field_types.toString()+"\n");
    System.out.print("\n");
    System.out.print("The first field is of name " + schema.getFieldName(0) + "\n");
    System.out.print("The second field is of type " + (schema.getFieldType(1)) + "\n");
    System.out.print("The field f3 is of type " + (schema.getFieldType("f3")) + "\n");
    System.out.print("The field f4 is at offset " + schema.getFieldOffset("f4") + "\n" + "\n");

    System.out.flush();
    
    //Error testing
//    System.out.print("Error testing: " + "\n");
//    schema.getFieldName(-1); // out of bound
//    schema.getFieldName(schema.getNumOfFields()); // out of bound
//    schema.getFieldType(-1); // out of bound
//    schema.getFieldType(schema.getNumOfFields()); // out of bound
//    schema.getFieldType("test"); // field name does not exist
//    schema.getFieldOffset("test"); // field name does not exist

//    field_names.add("f4"); // same field name
//    field_types.add(FieldType.STR20);
//    Schema schema_error=new Schema(field_names,field_types);
////
//    field_names.set(4,""); // empty field name
//    Schema schema_error2=new Schema(field_names,field_types);
////
//    field_names.set(4,"f5"); // corrects field name
//    field_names.add("f6");
//    field_names.add("f7");
//    field_names.add("f8");
//    field_names.add("f9");
//    field_types.add(FieldType.STR20);
//    field_types.add(FieldType.STR20);
//    field_types.add(FieldType.STR20);
//    field_types.add(FieldType.STR20);
//    Schema schema_error3=new Schema(field_names,field_types);
////
//    field_types.remove(field_types.size()-1);
//    Schema schema_error4=new Schema(field_names,field_types); // ArrayList sizes unmatched
////
//    ArrayList<String> vs=new ArrayList<String>();
//    ArrayList<FieldType> vf=new ArrayList<FieldType>();
//    Schema schema_error5=new Schema(vs,vf); // empty ArrayList
//
//    System.err.flush();
////    
//    //restore the fields for later use
//    field_names.subList(field_names.size()-5,field_names.size()).clear();
//    field_types.subList(field_types.size()-4,field_types.size()).clear();
//    System.out.print("\n");  
//
    //=====================Relation & SchemaManager=========================
    System.out.print("=====================Relation & SchemaManager=========================" + "\n");

    // Create a relation with the created schema through the schema manager
    String relation_name="ExampleTable1";
    System.out.print("Creating table " + relation_name + "\n");
    Relation relation_reference=schema_manager.createRelation(relation_name,schema);

    // Print the information about the Relation
    System.out.print("The table has name " + relation_reference.getRelationName() + "\n");
    System.out.print("The table has schema:" + "\n");
    System.out.print(relation_reference.getSchema() + "\n");
    System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
    System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");

    System.out.flush();
    
    // Error testing
    System.out.print("Error testing: " + "\n");
    schema_manager.createRelation(relation_name,schema); // create a relation with the same name
    schema_manager.createRelation("test",new Schema()); // create a relation with empty schema
    System.out.print("\n");

    System.err.flush();
    
    // Print the information provided by the schema manager
    System.out.print("Current schemas and relations: " + "\n");
    System.out.print(schema_manager + "\n");
    System.out.print("From the schema manager, the table " + relation_name + " exists: "
        + (schema_manager.relationExists(relation_name)?"TRUE":"FALSE") + "\n");
    System.out.print("From the schema manager, the table " + relation_name + " has schema:" + "\n");
    System.out.print(schema_manager.getSchema(relation_name) + "\n");
    System.out.print("From the schema manager, the table " + relation_name + " has schema:" + "\n");
    System.out.print(schema_manager.getRelation(relation_name).getSchema() + "\n");

    System.out.print("Creating table ExampleTable2 with the same schema" + "\n");
    schema_manager.createRelation("ExampleTable2",schema);
    System.out.print("After creating a realtion, current schemas and relations: " + "\n");
    System.out.print(schema_manager + "\n");

    System.out.print("Creating table ExampleTable3 with a different schema" + "\n");
    field_types.set(1,FieldType.INT);
    Schema schema3=new Schema(field_names,field_types);
    System.out.print("The schema has field names: " + "\n");
    field_names=schema3.getFieldNames();
    System.out.print(field_names.toString()+"\n");
    System.out.print("The schema has field types: " + "\n");
    field_types=schema3.getFieldTypes();
    System.out.print(field_types.toString()+"\n");
    relation_reference = schema_manager.createRelation("ExampleTable3",schema3);
    System.out.print("After creating a realtion, current schemas and relations: " + "\n");
    System.out.print(schema_manager + "\n");

    System.out.print("Deleting table ExampleTable2" + "\n");
    schema_manager.deleteRelation("ExampleTable2");
    System.out.print("After deleting a realtion, current schemas and relations: " + "\n");
    System.out.print(schema_manager + "\n" + "\n");

    System.out.flush();
    
    //Error testing
    System.out.print("Error testing: " + "\n");
    System.out.print("The table ExampleTable2 exists: " + (schema_manager.relationExists("ExampleTable2")?"TRUE":"FALSE") + "\n");
    schema_manager.createRelation("",schema); // empty relation name
    schema_manager.getSchema("ExampleTable2"); // relation does not exist
    schema_manager.getRelation("ExampleTable2"); // relation does not exist
    schema_manager.deleteRelation("ExampleTable2"); // relation does not exist
    System.out.print("\n");

    System.err.flush();
    
    //====================Tuple=============================
    System.out.print("====================Tuple=============================" + "\n");

    // Set up the first tuple of ExampleTable3
    Tuple tuple = relation_reference.createTuple(); //The only way to create a tuple is to call "Relation"
    tuple.setField(0,"v11");
    tuple.setField(1,21);
    tuple.setField(2,31);
    tuple.setField(3,"v41");

    // Another way of setting the tuples
    tuple.setField("f1","v11");
    tuple.setField("f2",21);
    tuple.setField("f3",31);
    tuple.setField("f4","v41");

    // Print the information about the tuple
    System.out.print("Created a tuple " + tuple + " of ExampleTable3 through the relation" + "\n");
    System.out.print("The tuple is invalid? " + (tuple.isNull()?"TRUE":"FALSE") + "\n");
    Schema tuple_schema = tuple.getSchema();
    System.out.print("The tuple has schema" + "\n");
    System.out.print(tuple_schema + "\n");
    System.out.print("A block can allow at most " + tuple.getTuplesPerBlock() + " such tuples" + "\n");

    System.out.print("The tuple has fields: " + "\n");
    for (int i=0; i<tuple.getNumOfFields(); i++) {
      if (tuple_schema.getFieldType(i)==FieldType.INT)
        System.out.print(tuple.getField(i) + "\t");
      else
        System.out.print(tuple.getField(i) + "\t");
    }
    System.out.print("\n");

    System.out.print("The tuple has fields: " + "\n");
    System.out.print(tuple.getField("f1") + "\t");
    System.out.print(tuple.getField("f2") + "\t");
    System.out.print(tuple.getField("f3") + "\t");
    System.out.print(tuple.getField("f4") + "\t");
    System.out.print("\n" + "\n");

    System.out.flush();
    
    //Error testing
    System.out.print("Error testing: " + "\n");
    tuple.setField(0,11); // wrong type of value
    tuple.setField(-1,"v11"); // out of bound
    tuple.setField(tuple.getNumOfFields(),"v11"); // out of bound
    tuple.setField("f2","v21"); // wrong type of value
    tuple.setField("f5",21); // field does not exist
    tuple.getField(-1); // out of bound
    tuple.getField(tuple.getNumOfFields()); // out of bound
    System.out.print("\n");

    System.err.flush();
    
    //===================Block=============================
    System.out.print("===================Block=============================" + "\n");
    // Set up a block in the memory
    System.out.print("Clear the memory block 0" + "\n");
    Block block_reference=mem.getBlock(0); //access to memory block 0
    block_reference.clear(); //clear the block

    // A block stores at most 2 tuples in this case
    // -----------first tuple-----------
    System.out.print("Set the tuple at offset 0 of the memory block 0" + "\n");
    block_reference.setTuple(0,tuple); // You can also use appendTuple()
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n");

    System.out.print("The block is full? " + (block_reference.isFull()?"true":"false") + "\n");
    System.out.print("The block currently has " + block_reference.getNumTuples() + " tuples" + "\n");
    System.out.print("The tuple at offset 0 of the block is:" + "\n");
    System.out.print(block_reference.getTuple(0) + "\n" + "\n");

    // -----------second tuple-----------
    System.out.print("Append the same tuple to the memory block 0" + "\n");
    block_reference.appendTuple(tuple);
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n");

    System.out.print("The block is full? " + (block_reference.isFull()?"true":"false") + "\n");
    System.out.print("The block currently has " + block_reference.getNumTuples() + " tuples" + "\n");
    System.out.print("The tuple at offset 0 of the block is:" + "\n");
    System.out.print(block_reference.getTuple(0) + "\n");

    ArrayList<Tuple> tuples=block_reference.getTuples();
    System.out.print("Again the tuples in the memory block 0 are:" + "\n");
    for (int i=0;i<tuples.size();i++)
    	System.out.print(tuples.get(i).toString()+"\n");

    // -----------erase and add-----------
    System.out.print("Erase the first tuple" + "\n");
    block_reference.invalidateTuple(0);
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n");

    System.out.print("Erase all the tuples in the block" + "\n");
    block_reference.invalidateTuples();
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n");

    System.out.print("(Remove all tuples;) Set only the first tuple" + "\n");
    block_reference.setTuples(tuples,0,1);
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n" + "\n");

    System.out.print("(Remove all tuples;) Set the same two tuples again" + "\n");
    block_reference.setTuples(tuples);
    System.out.print("Now the memory block 0 contains:" + "\n");
    System.out.print(block_reference + "\n");

    System.out.flush();
    
    //Error testing
    System.out.print("Error testing: " + "\n");
    Tuple tuple2 = schema_manager.getRelation("ExampleTable1").createTuple();
    tuple2.setField(0,"v11");
    tuple2.setField(1,"v21");
    tuple2.setField(2,31);
    tuple2.setField(3,"v41");
    block_reference.setTuple(1,tuple2); // different schemas

    block_reference.setTuple(-1,tuple); //out of bound
    block_reference.setTuple(tuple.getTuplesPerBlock(),tuple); //out of bound
    block_reference.getTuple(-1); //out of bound
    block_reference.getTuple(tuple.getTuplesPerBlock()); //out of bound
    block_reference.invalidateTuple(-1); //out of bound
    block_reference.invalidateTuple(tuple.getTuplesPerBlock()); //out of bound
    block_reference.appendTuple(tuple); // append a tuple to a full block
    System.out.print("\n");

    System.err.flush();
    
    //======How to append tuples to the end of the relation======
    System.out.print("\n" + "======How to append tuples to the end of the relation======" + "\n");

    // ---------Append the fisrt tuple---------

    System.out.print("Now memory contains: " + "\n");
    System.out.print(mem + "\n");

    // append the tuple to the end of the ExampleTable3 using memory block 5 as output buffer
    System.out.print(relation_reference + "\n" + "\n");
    appendTupleToRelation(relation_reference,mem,5,tuple);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");

    // ---------Set the second tuple---------

    System.out.print("Create the second tuple " + "\n");
    tuple.setField("f1","v12");
    tuple.setField("f2",22);
    tuple.setField("f3",32);
    tuple.setField("f4","v42");
    System.out.print(tuple + "\n");

    // append the tuple to the end of the ExampleTable3 using memory block 5 as output buffer
    appendTupleToRelation(relation_reference,mem,5,tuple);
    System.out.print("*NOTE: The example here does not consider empty tuples (if any) in the block." + "\n");
    System.out.print("(The holes left after tuple deletion)" + "\n");

    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");

    // ---------Set the thrid tuple---------

    System.out.print("Create the third tuple " + "\n");
    tuple.setField("f1","v13");
    tuple.setField("f2",23);
    tuple.setField("f3",33);
    tuple.setField("f4","v43");
    System.out.print(tuple + "\n");

    // append the tuple to the end of the ExampleTable3 using memory block 5 as output buffer
    appendTupleToRelation(relation_reference,mem,5,tuple);
    System.out.print("*NOTE: The example here does not consider empty tuples (if any) in the block." + "\n");
    System.out.print("(The holes left after tuple deletion)" + "\n");
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");

    //======How to handle bulk blocks======
    System.out.print("\n" + "======How to read and write bulk blocks======" + "\n");

    System.out.print("First fill the relations with 10 more tuples" + "\n");
    for (int i=0;i<10;i++) {
      appendTupleToRelation(relation_reference,mem,5,tuple);
    }
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Read bulk blocks from the relation to memory block 3-9" + "\n");
    relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Write bulk blocks from the memory block 3-9 to the end of the relation" + "\n");
    System.out.print("(May result in 'holes' in the relation)" + "\n");
    relation_reference.setBlocks(relation_reference.getNumOfBlocks(),3,9-3+1);
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n");

    System.out.print("Deleting the last 7 blocks of the relation" + "\n");
    relation_reference.deleteBlocks(7);
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");

//    ======How to delete tuples from the relation======
    System.out.print("\n" + "======How to delete tuples from the relation======" + "\n");

    // ---------Erase one tuple---------

    System.out.print("Reading the first block of the relation into memory block 1:" + "\n");
    relation_reference.getBlock(0,1);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Deleting the tuple at offset 0 of the memory block 1" + "\n");
    block_reference=mem.getBlock(1);
    block_reference.invalidateTuple(0);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Writing memory block 1 back to the first block of the relation" + "\n");
    relation_reference.setBlock(0,1);
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n");

    // ---------Erase another tuple---------

    System.out.print("Reading the last block of the relation into memory block 1:" + "\n");
    relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,1);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Emptying the tuples at the memory block 1" + "\n");
    block_reference=mem.getBlock(1);
    block_reference.invalidateTuples();
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Writing memory block 1 back to the last block of the relation" + "\n");
    relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,1);
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n");

    // ---------Delete Block---------

    System.out.print("Deleting the last block of the relation to remove trailing space" + "\n");
    relation_reference.deleteBlocks(relation_reference.getNumOfBlocks()-1);
    System.out.print("Now the relation contains: " + "\n");
    System.out.print(relation_reference + "\n" + "\n");

    System.out.flush();
    
    //Error testing of Relation
    System.out.print("Error testing of Relation: " + "\n");
    relation_reference.getBlock(-1,5); //out of bound
    relation_reference.getBlock(relation_reference.getNumOfBlocks(),5); //out of bound
    relation_reference.getBlock(0,-1); //out of bound
    relation_reference.getBlock(0,mem.getMemorySize()); //out of bound
    relation_reference.getBlocks(-1,5,1); //out of bound
    relation_reference.getBlocks(relation_reference.getNumOfBlocks(),5,1); //out of bound
    relation_reference.getBlocks(0,-1,1); //out of bound
    relation_reference.getBlocks(0,mem.getMemorySize(),1); //out of bound
    relation_reference.getBlocks(0,5,6); //out of memory bound
    relation_reference.getBlocks(6,5,2); //out of relation bound
    relation_reference.setBlock(-1,5); //out of bound
    relation_reference.setBlock(0,-1); //out of bound
    relation_reference.setBlock(0,mem.getMemorySize()); //out of bound
    relation_reference.setBlocks(-1,5,1); //out of bound
    relation_reference.setBlocks(0,-1,1); //out of bound
    relation_reference.setBlocks(0,mem.getMemorySize(),1); //out of bound
    relation_reference.setBlocks(0,5,6); //out of memory bound
//
    relation_reference.deleteBlocks(-1); //out of bound
    relation_reference.deleteBlocks(relation_reference.getNumOfBlocks()); //out of bound
    schema_manager.getRelation("ExampleTable1").setBlock(0,0); // different table schemas
    schema_manager.getRelation("ExampleTable1").setBlocks(0,0,1); // different table schemas
    System.out.print("\n");

    System.err.flush();
    
    //===================Memory=============================
    System.out.print("===================Memory=============================" + "\n");

    System.out.print("Reading the first block of the relation into memory block 9:" + "\n");
    relation_reference.getBlock(0,9);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Copy the memory block 9 to memory block 6-8:" + "\n");
    System.out.print("(You might not need this function)" + "\n");
    mem.setBlock(6,mem.getBlock(9));
    mem.setBlock(7,mem.getBlock(9));
    mem.setBlock(8,mem.getBlock(9));
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.print("Get the tuples in memory block 6-9" + "\n");
    System.out.print("(Can apply sorting and heap building to the tuples later):" + "\n");
    tuples=mem.getTuples(6,4);
    for (int i=0;i<tuples.size();i++)
    	System.out.print(tuples.get(i).toString()+"\n");
    System.out.println("");

    System.out.print("Write the 'condensed' tuples to memory block 1-2:" + "\n");
    mem.setTuples(1,tuples);
    System.out.print("Now the memory contains: " + "\n");
    System.out.print(mem + "\n");

    System.out.flush();
    
    //Error testing
    System.out.print("Error testing: " + "\n");
    mem.getBlock(-1); //out of bound
    mem.getBlock(mem.getMemorySize()); //out of bound
    mem.setBlock(-1,mem.getBlock(9)); //out of bound
    mem.setBlock(mem.getMemorySize(),mem.getBlock(9)); //out of bound
    mem.getTuples(-1,4); //out of bound
    mem.getTuples(mem.getMemorySize(),4); //out of bound
    mem.getTuples(0,0); // get 0 block
    mem.getTuples(6,5); // get too many blocks
    mem.setTuples(-1,tuples); //out of bound
    mem.setTuples(mem.getMemorySize(),tuples); //out of bound

    //Store a tuple of ExampleTable2 at memory block 2
    mem.getBlock(2).clear();
    mem.getBlock(2).setTuple(0,tuple2);
    mem.getTuples(1,2); //Error: get memory block 1-2, which contain tuples of different tables

    System.err.flush();
    
    long elapsedTimeMillis = System.currentTimeMillis()-start; 
    System.out.print("Computer elapse time = " + elapsedTimeMillis + " ms" + "\n");
    System.out.print("Calculated elapse time = " + disk.getDiskTimer() + " ms" + "\n");
    System.out.print("Calculated Disk I/Os = " + disk.getDiskIOs() + "\n");
  }

}
