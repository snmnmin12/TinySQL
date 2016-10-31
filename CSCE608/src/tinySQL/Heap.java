package tinySQL;

import java.util.Arrays;
public class Heap<T extends Comparable<T>> {
	private static final int DEFAULT_CAPACITY = 10;
	private T[] array;
	private int size;
	
	@SuppressWarnings("unchecked")
	public Heap() {
		//constructor
		array = (T[]) new Comparable[DEFAULT_CAPACITY];
		size = 0;
	}

	 @SuppressWarnings("unchecked")
	public Heap(T[] arr)
	   {
	      size = arr.length;
	      array = (T[]) new Comparable[size+1];
	      System.arraycopy(arr, 0, array, 1, size);
	      buildHeap();
	      int n = size;
	      while(n > 1) {
	    	swap(1, n--);
	    	siftdown(1, n);
	      }
	      System.arraycopy(array, 1, arr, 0, array.length-1);
	   }
	
	
	public void insert(T item) {
		if (size == array.length - 1) {
			array = resize();
		}
		//insert element into heap
		size++;
		int index = size;
		array[index] = item;
		swimup();
	}
	
	public T remove() {
		if (isEmpty()) {
			throw new IllegalStateException();
		}
		T small = array[1];
		array[1] = array[size];
    	array[size] = null;
    	size--;
    	siftdown(1, size);
    	return small;
	}
	
    public String toString() {
        return Arrays.toString(Arrays.copyOfRange(array, 1, size));
    }
    
	public int getSize() {
		return size;
	}
	
	//check the array empty
    public boolean isEmpty() {
        return size == 0;
    }
    
	//private helper method to find children and parent
	private void swap(int i, int j) {
		T temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
	
	//build heap for the array input
    private void buildHeap() {
    	for (int k = size/2; k > 0; k--) {
    		siftdown(k, size);
    	}
    }
	
    //This is to build a min-heap by insertion
	private void swimup() {
	    int index = size;
	    int parent = index/2;
	    while (index>1 && (array[parent].compareTo(array[index]) > 0)) {
	        swap(index, parent);
	        index = parent;
	        parent = parent/2;
	    }        	
	}
	
	//build a max-heap first, then remove the max element to sort it
	private void siftdown(int k, int n) {
	  while (2*k <= n) {
		int j = 2*k;
		if (j < n && array[j].compareTo(array[j+1]) < 0) j++;
		if (array[k].compareTo(array[j]) >= 0) break;
		swap(k,j);
		k = j;
	  }
	  
	}
	//resize the array if required
	private T[] resize() {
		return Arrays.copyOf(array, array.length*2);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    Heap<String> h = new Heap<String>();
	      h.insert("p");
	      h.insert("r");
	      h.insert("i");
	      h.insert("o");
	      System.out.println(h);
	      h.remove();
	      System.out.println(h);

		Heap<Integer> h2 = new Heap<Integer>();
		Integer[] a = {488,667,634,380,944,594,783,584,550,665,721,819,285,344,503,807,491,623,845,300};
		for (int i = 0; i < a.length; i++)
			h2.insert(a[i]);
		System.out.println(h2);
	      
	      Integer[] a2 = {4,7,7,7,5,0,2,3,5,1};
	      Heap<Integer> tmp = new Heap<Integer>(a2);
	      System.out.println(Arrays.toString(a2));
	}

}
