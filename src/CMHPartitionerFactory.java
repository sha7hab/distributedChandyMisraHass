//package org.apache.giraph.partition;

import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


@SuppressWarnings("rawtypes")

public class CMHPartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends GraphPartitionerFactory<I, V, E> {

  @Override
  public int getPartition(I id, int partitionCount, int workerCount) {

		int Part =  Math.abs(id.hashCode() % partitionCount);
		int nodeId = id.hashCode();
		
		switch (nodeId)	{
	
				case 0:
				case 1:
				case 2:
		    		Part = 0;
					break;	    
		
				case 3:
				case 4:
				case 5:
					Part = 1;
		  			break;	
	  
				case 6:
				case 7:
				case 8:
					Part = 2;
		  		break;	
			}
    return Part;
  }

  @Override
  public int getWorker(int partition, int partitionCount, int workerCount) {
    return partition % workerCount;
  }
}