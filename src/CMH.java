
import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

@Algorithm(
    name = "Chandy Misra Haas",
    description = "Finds the root of distributed deadlock!"
)
public class CMH extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("CMH.sourceId", 0,
          "The source id");
  
  public static boolean DeadDetect = false;
  public static boolean MaxDetect = false;

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }
  
  

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
	  
	  double src = -1;
	  double max = vertex.getId().get();
	  
	  if (getSuperstep() == 0) {
		  vertex.setValue(new DoubleWritable(0)); 
		  if (isSource(vertex) == true)
		  {
			  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				  double srcid = vertex.getId().get();
				  sendMessage(edge.getTargetVertexId(), new DoubleWritable(srcid));
			  }
		  }
    }
	  
	
	if (!DeadDetect)
	{
    for (DoubleWritable message : messages) {

      if (message.get() == -10.0 && isSource(vertex))
      {
    	  vertex.setValue(new DoubleWritable(0));
    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
			  double srcid = vertex.getId().get();
			  sendMessage(edge.getTargetVertexId(), new DoubleWritable(srcid));
		  }
      }
      
      double tmp = message.get();
      double dblid = vertex.getId().get();
      if (tmp == dblid)
      {
    	  System.out.println("DeadLock Detected!");
    	  vertex.setValue(new DoubleWritable(1));
    	  DeadDetect = true;
    	  
    	  
    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
			  double srcid = vertex.getId().get();
			  sendMessage(edge.getTargetVertexId(), new DoubleWritable(srcid));
		  }
    	  
    	  break;
    	  
      }
      src = tmp;
    }
    
    
      if (!isSource(vertex) && src != -1 && vertex.getValue().get() != 1) {
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

          System.out.println(vertex.getId() + " is sending " + src + " - " + edge.getTargetVertexId());
          sendMessage(edge.getTargetVertexId(), new DoubleWritable(src));
          
       }
       
      vertex.setValue(new DoubleWritable(1));
      
    }
	}
	else
	{
		//System.out.println("YES!"+src);
		for (DoubleWritable message : messages) {  
		  double tmp = message.get();
	      double dblid = vertex.getId().get();
	      if (tmp == dblid)
	      {
	    	  System.out.println(vertex.getId().get()+" The Biggest UID Detected!");
	    	  vertex.setValue(new DoubleWritable(10));
	    	  MaxDetect = true;
	    	  
	    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				  double srcid = -10.0;
				  sendMessage(edge.getTargetVertexId(), new DoubleWritable(srcid));
			  }
	    	  
	    	  //removeVertexRequest(vertex.getId());
	    	  
	    	  //for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
	    		  
	    		  //removeEdgesRequest(vertex.getId(), edge.getTargetVertexId());
	    		//  vertex.removeEdges(edge.getTargetVertexId());
			  
	    	  //}
	    	  
	    	  
	    	  DeadDetect = false;
	    	  
	    	  
	    	  
	    	  
	    	  break;
	    	  
	      }
	      
	      if  (max < tmp)
	        {max = tmp;}
	    }
	    
	    
	      if (!MaxDetect) {
	      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

	          System.out.println(vertex.getId() + " helloois sending " + max + " - " + edge.getTargetVertexId());
	          sendMessage(edge.getTargetVertexId(), new DoubleWritable(max));
	          
	       }
	       
	      }
	      
	}
    
	vertex.voteToHalt();
  }
}
