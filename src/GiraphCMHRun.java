import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
//import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.job.GiraphJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner;


public class GiraphCMHRun implements Tool{
	
	private Configuration conf;
	public Configuration getConf() {
		return conf;
		}
	
	public void setConf(Configuration conf) { 
		this.conf = conf;
	}
	
	public int run(String[] args) throws Exception { 
		String inputPath="/Users/sha7hab/eclipse-workspace/graphInput/inputGraph.txt"; 
		String outputPath="/Users/sha7hab/graphOutput"; 
		
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		giraphConf.setComputationClass(FRSP.class);
		giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(giraphConf ,new Path(inputPath));
		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		giraphConf.setLocalTestMode(true);
		giraphConf.setWorkerConfiguration(1, 1, 100);
		GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf ,false);
		InMemoryVertexOutputFormat.initializeOutputGraph(giraphConf);
		GiraphJob giraphJob = new GiraphJob(giraphConf ,"GiraphRun");
		FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));
		giraphJob.run(true);
		return 0;
	}
public static void main(String[] args) throws Exception{
	ToolRunner.run(new GiraphCMHRun(), args);
	}
}