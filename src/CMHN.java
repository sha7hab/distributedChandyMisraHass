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
    description = "Finds distributed deadlock!"
)
public class CMHN extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The source id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("CMH.sourceId", 0,
          "The source id");
  

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
	   
	  
	// initialization
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
	
	// Detecting Deadlock
	if (getSuperstep() > 0 && vertex.getValue().get() == 0) {
	
    for (DoubleWritable message : messages) {
      
      double msg = message.get();
      double dblid = vertex.getId().get();
      if (msg == dblid)
      {
    	  System.out.println("DeadLock Detected! " + getTotalNumVertices());
    	  vertex.setValue(new DoubleWritable(1));
		  
    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

    			sendMessage(edge.getTargetVertexId(), new DoubleWritable(-10));
    			System.out.println("sendinggg " + getSuperstep());
    		  }
    	  
		  break;
	  }
	  else
	  {
	  if (isSource(vertex) == false) {
	  
	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

		sendMessage(edge.getTargetVertexId(), new DoubleWritable(msg));
		
	  }
	  
	  if(vertex.getValue().get()!=-100)
		{
		  vertex.setValue(new DoubleWritable(1));
		}
	  
	  
	  vertex.voteToHalt();
	  }
	  
	  }

    }

    }
	
	
	
	// Tell Others That Deadlock Detected
	
	if (getSuperstep() > 0 && vertex.getValue().get() == 1) {
		
	    for (DoubleWritable message : messages) {
	      
	      double msg = message.get();
	      double dblid = vertex.getId().get();
	      if (msg == -10 && isSource(vertex) == true)
	      {
	    	  System.out.println("DeadLock Detected! " + getTotalNumVertices());
	    	  vertex.setValue(new DoubleWritable(-10));
			  
	    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

	    			sendMessage(edge.getTargetVertexId(), new DoubleWritable(dblid));
	    			
	    		  }
	    	  
			  break;
		  }
		  else
		  {
		  if (msg == -10) {
		  if (isSource(vertex) == false) {
		  
		  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

			  
			sendMessage(edge.getTargetVertexId(), new DoubleWritable(msg));
			
		  }
		  vertex.setValue(new DoubleWritable(-10));
		  vertex.voteToHalt();
		  }
	      }
		  }

	    }

	 }
	
	
	
	// Detecting the maximum ID
	
	
	if (getSuperstep() > 0 && vertex.getValue().get() == -10) {
		
		double max = vertex.getId().get(); 
		
		
	    for (DoubleWritable message : messages) {
	      
	      double msg = message.get();
	      double dblid = vertex.getId().get();
	      
	      if (msg == dblid && isSource(vertex)==false)
	      {
	    	  System.out.println("Max Detected! " + getTotalNumVertices());
	    	  vertex.setValue(new DoubleWritable(-100));
	    	  
	    	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

	    			sendMessage(edge.getTargetVertexId(), new DoubleWritable(-20));
	    			
	    		  }
	    	  
	    	  
			  break;
		  }
		  else
		  {
		  
		  if (msg > max)
		  {
			  max = msg;
		  }
		  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

			sendMessage(edge.getTargetVertexId(), new DoubleWritable(max));
			System.out.println("nodde "+vertex.getId().get()+" send "+max+ " to "+edge.getTargetVertexId());
			
		  }
		  //vertex.setValue(new DoubleWritable(1));
		  //vertex.voteToHalt();
		  
		  
		  }

	    }

	    }
	
		// request run again
	
	
		if (getSuperstep() == getTotalNumVertices()*3 && vertex.getValue().get()==-10)
		{
			if(vertex.getValue().get()!=-100)
			{
				System.out.println("hello"+vertex.getId().get());
				vertex.setValue(new DoubleWritable(0));
			}
			if (isSource(vertex) == true)
			  {
				for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
					  double srcid = vertex.getId().get();
					  sendMessage(edge.getTargetVertexId(), new DoubleWritable(srcid));
				  }
			  }
		}
		
		
		
		// remove maximum ID
		
	    if (getSuperstep() == getTotalNumVertices()*4)
	    {
	    	if(vertex.getValue().get()==-100)
			{
				removeVertexRequest(vertex.getId());
			}
	    	
	    }
	    
	    
	    // Finish
	    
	    if (getSuperstep() == getTotalNumVertices()*4+1)
	    {
	    vertex.voteToHalt();
	    }
	    
	    System.out.println("noddde "+vertex.getId().get()+" ss "+getSuperstep());
	    
	    //if (getSuperstep() == 95)
	    //	{vertex.voteToHalt();}

    }
}
