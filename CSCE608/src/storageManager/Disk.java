package storageManager;

import java.io.Serializable;
import java.util.ArrayList;

/* Simplified assumptions are made for disks. A disk contains many tracks. 
 * We assume each relation reside on a single track of blocks on disk. 
 * Everytime to read or write blocks of a relation takes time:
 *
 * (AVG_SEEK_TIME + AVG_ROTATION_LATENCY + 
 * 		AVG_TRANSFER_TIME_PER_BLOCK * num_of_consecutive_blocks)
 *
 * The number of disk I/O is calculated by the number of 
 *   blocks read or written.
 * Usage: At the beginning of your program, you need to 
 *          initialize a disk.
 *       You don't need to access Disk directly except for 
 *          getting disk I/O counts
 *       When you need to access a relation, use the Relation class
 */

public class Disk implements Serializable {
    private final static int NUM_TRACKS=100;
    //Properties are defined based on the Megatron 747 disk sold in 2001.
    
    //One block holds 16384 bytes (although a block only holds 8 fields in here)
    //Thus, a relation of 60 tuples/blocks occupies as much as 960K
    //If memory has 1/6 of the relation size, then the memory has only 160K space
    //However, we want to simulate the speed of a 300M relation and a 50M memory
    //So we increase the transfer time of a block by 320 folds
    private final static double avg_seek_time=6.46;
    private final static double avg_rotation_latency=4.17;
    private final static double avg_transfer_time_per_block=0.20 * 320;

    protected ArrayList<ArrayList<Block>> tracks;
    private long diskIOs=0;
    private double timer=0;
    
	public Disk() {
	    resetDiskIOs(); resetDiskTimer();
	    tracks=new ArrayList<ArrayList<Block>>(NUM_TRACKS);
	    for (int i=0;i<NUM_TRACKS;i++)
	      tracks.add(new ArrayList<Block>());
	}

 // for internal use: extend the track to 'block_index'-1; 
    // no disk latency
  protected boolean extendTrack(int schema_index, int block_index, 
		  						Tuple t) {
    if (block_index<0) {
      System.err.print("extendTrack ERROR: block index " 
    		  		+ block_index + " out of disk bound" + "\n");
      return false;
    }
    ArrayList<Block> track=tracks.get(schema_index);
    int j=track.size();
    if (block_index>j) {
      if (j>0) {
    	// first fill the last block with invalid tuples
        while (!track.get(j-1).isFull()) { 
          track.get(j-1).appendTuple(new Tuple(t));
        }
      }
      // fill the gap with invalid tuples
      for (int i=j;i<block_index-1;i++) {
        track.add(new Block());
        while (!track.get(i).isFull()) {
          track.get(i).appendTuple(new Tuple(t));
        }
      }
      // fill the last block with only one invalid tuple
      track.add(new Block());
      track.get(block_index-1).appendTuple(new Tuple(t));
    }
    return true;
  }

  //for internal use: shrink the track to 'block_index'-1; 
  // no disk latency
  protected boolean shrinkTrack(int schema_index, int block_index) {
    if (block_index<0 || 
    		block_index >= tracks.get(schema_index).size()) {
      System.err.print("shrinkTrack ERROR: block index " 
    		  			+ block_index + " out of disk bound" + "\n");
      return false;
    }
    tracks.get(schema_index).subList(block_index,
    								tracks.get(schema_index).size()).clear();
    return true;
  }
  
  //for internal use
  protected Block getBlock(int schema_index, int block_index) {
    if (block_index<0 || 
    		block_index>=tracks.get(schema_index).size())  {
      System.err.print("getBlock ERROR: block index " 
    		  			+ block_index + " out of disk bound" + "\n");
      return new Block();
    }
    incrementDiskIOs(1);
    incrementDiskTimer(1);

    return new Block(tracks.get(schema_index).get(block_index));
  }
  
  //for internal use
  protected ArrayList<Block> getBlocks(int schema_index, 
		  							int block_index, int num_blocks) {
    if (block_index<0 || block_index>=tracks.get(schema_index).size())  {
      System.err.print("getBlocks ERROR: block index " 
    		  			+ block_index + " out of disk bound" + "\n");
      return new ArrayList<Block>();
    }
    int i;
    if ((i=block_index+num_blocks-1)>=tracks.get(schema_index).size()) {
      System.err.print("getBlocks ERROR: num of blocks " +
      						" out of disk bound: " + i + "\n");
      return new ArrayList<Block>();
    }
    incrementDiskIOs(num_blocks);
    incrementDiskTimer(num_blocks);

    ArrayList<Block> v=new ArrayList<Block>(num_blocks);
    for (i=block_index;i<block_index+num_blocks;i++ ){
      v.add(tracks.get(schema_index).get(i));
    }
    return v;
  }
  
  //for internal use
  protected boolean setBlock(int schema_index, int block_index, 
		  					Block b) {
    if (block_index<0)  {
      System.err.print("setBlock ERROR: block index " + block_index 
    		  			+ " out of disk bound" + "\n");
      return false;
    }
    incrementDiskIOs(1);
    incrementDiskTimer(1);
    tracks.get(schema_index).set(block_index,new Block(b));
    return true;
  }

  //for internal use
  protected boolean setBlocks(int schema_index, int block_index, 
		  					ArrayList<Block> vb) {
    if (block_index<0)  {
      System.err.print("setBlocks ERROR: block index " + block_index 
    		  			+ " out of disk bound" + "\n");
      return false;
    }
    incrementDiskIOs(vb.size());
    incrementDiskTimer(vb.size());
    int i,j;
    for (i=0,j=block_index;i<vb.size();i++,j++)
      tracks.get(schema_index).set(j,new Block(vb.get(i)));
    return true;
  }

  //for internal use: increment Disk I/O count
  protected void incrementDiskIOs(int count) {
    if (Config.DISK_I_O_DEBUG)
      System.err.print("DEBUG: Disk I/O is incremented by " 
    		  			+ count + "\n");
    diskIOs+=count;
  }

  //for internal use: increment Disk time
  protected void incrementDiskTimer(int num_blocks) {
    if (Config.SIMULATED_DISK_LATENCY_ON) {
      try {
        Thread.sleep((long)(avg_seek_time+avg_rotation_latency
        					+avg_transfer_time_per_block*num_blocks));
      } catch (Exception e) {
          System.out.print(e);
      }
    }

    timer+=avg_seek_time+avg_rotation_latency
    		+avg_transfer_time_per_block*num_blocks;
  }

  // Reset the disk I/O counter.
  // Every time before you do a SQL operation, reset the counter.  
  public void resetDiskIOs() {
    diskIOs=0;
  }

  // After the operation is done, get the accumulated number 
  // of disk I/Os  
  public long getDiskIOs()  {
    return diskIOs;
  }

  // Reset the disk timmer.
  // Every time before you do a SQL operation, reset the timmer.  
  public void resetDiskTimer() {
    timer=0;
  }

  // After the operation is done, get the elapse disk time 
  // in milliseconds  
  public double getDiskTimer()  {
    return timer;
  }    
}
