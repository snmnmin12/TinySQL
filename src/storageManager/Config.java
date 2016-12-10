package storageManager;

public class Config {
	// Therefore, a block can hold 1-8 tuples depending on the relation schema.
    public static int FIELDS_PER_BLOCK = 8;     
    public static int MAX_NUM_OF_FIELDS_IN_RELATION = 8;
    // Starts with small memory to test one-pass and two-pass algorithms
   public static int NUM_OF_BLOCKS_IN_MEMORY = 10; 
    

    //Use this value instead to measure algorithm performance 
    // on 1000-tuple relation
    //public static int NUM_OF_BLOCKS_IN_MEMORY = 300;

  //Setting true turns on the simulated disk latency
    public static boolean SIMULATED_DISK_LATENCY_ON = true; 
  //Setting to true turns on the debug message of disk I/O incrementation
    public static boolean DISK_I_O_DEBUG = false; 

}
