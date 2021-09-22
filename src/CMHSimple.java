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
public class CMHSimple extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The source id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("CMH.sourceId", 13,
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
	
	if (getSuperstep() > 0) {
	
    for (DoubleWritable message : messages) {
      
      double msg = message.get();
      double dblid = vertex.getId().get();
      if (msg == dblid)
      {
    	  System.out.println("DeadLock Detected!");
    	  vertex.setValue(new DoubleWritable(2));
		  
		  break;
	  }
	  else
	  {
	  if (vertex.getValue().get() == 0 && isSource(vertex) == false) {
	  
	  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

		sendMessage(edge.getTargetVertexId(), new DoubleWritable(msg));
		
	  }
	  vertex.setValue(new DoubleWritable(1));
	  vertex.voteToHalt();
	  }
	  }

    }

    }
	
	vertex.voteToHalt();
	
    }
}
