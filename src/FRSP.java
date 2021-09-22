import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;



@Algorithm(
    name = "Fog Rational Service Placer",
    description = "Fog Service Placement Problem Considering Rational Behaviour"
)
public class FRSP extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The source id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("FRSP.sourceId", 0,
          "The source id");
  
  static int[] MSctr = new int[100];
  static int[][][] MS = new int[100][100][100];
  static int[][][][] MSData = new int[100][100][100][100];
  static int[][] FogNCap = new int [100][100];
  boolean DataSection = true;
  static boolean[] Uptack = new boolean[100];
  

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
	  
	  int ID = Math.toIntExact(vertex.getId().get());
	  int N = 9;
	  
	  
	  // initialization
	  if (getSuperstep() == 0) 
	  {
	  
      MSctr[ID] = 0;

	  // Reading Micro-services Data from File
	  
	  String ReadfileName = "/Users/sha7hab/eclipse-workspace/graphInput/MicroServices.txt";
	  String line = null;
	  
	  try {
		  
		  // FileReader reads text files in the default encoding.
          FileReader fileReader = new FileReader(ReadfileName);

          // Always wrap FileReader in BufferedReader.
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          
          int src = -1;
          int dst = -1;
          
          String[] lines;
          
          

          while((line = bufferedReader.readLine()) != null) {
        	   
          	lines = line.split(" ");
          	
          	if (lines[0].equals("{"))
          	{
          		MSctr[ID]++;
          		DataSection = true;
          	}
          	else
          	{          		 
          		if (lines[0].equals("},") || lines[0].equals("}"))
              	{
              		src = -1;
              		dst = -1;
              	}
          		else if (lines[0].equals(","))
              	{
          			DataSection = false;
              	}
          		else if (DataSection)
          		{
          			MSData[ID][MSctr[ID]][Integer.parseInt(lines[0])][0] = Integer.parseInt(lines[1]);
          			MSData[ID][MSctr[ID]][Integer.parseInt(lines[0])][1] = Integer.parseInt(lines[2]);
          			MSData[ID][MSctr[ID]][Integer.parseInt(lines[0])][3] = Integer.parseInt(lines[3]);
          			System.out.println(ID+ "<-id" + MSctr[ID] + "---00>" + lines[0] + "-0-==-0-" + lines[1]+ "-0-==-0-" + lines[2]);
          		}
          		else if (!DataSection)
          		{
                  	src = Integer.parseInt(lines[0]);
                    dst = Integer.parseInt(lines[1]);
                    	
                    MS[MSctr[ID]][src][dst] = 1;
                    MS[MSctr[ID]][dst][src] = 1;
                    	
                    System.out.println(src + "--->" + dst + "--==--" + MSctr[ID]);
          		}         		
          	}       	 
          }
      
          // Always close files.
          bufferedReader.close();     
	  }
	  
	  catch(FileNotFoundException ex) {
          System.out.println(
              "Unable to open file '" + 
              ReadfileName + "'");                
      }
      catch(IOException ex) {
          System.out.println(
              "Error reading file '" 
              + ReadfileName + "' or Error writing to file '");                  
          // Or we could just do this: 
          // ex.printStackTrace();
           
      }
	  
	  }
	  
	  
	  
	  
	  
	  if (getSuperstep() == 1) 
	  {
		 System.out.println("HELLLLLOOOOOOOOOOOOOOO= " + MSctr[ID]);
	  
		 for (int i = 1; i <= MSctr[ID]; i++) {
		  for (int j = 1; j <= 5; j++) {
			  for (int k = 1; k <= 5; k++)  
				  {
				  
				  //System.out.println(MS[i][j][k]);
				  
				  if (MS[i][j][k]==1) {
					  System.out.println(i + "--->" + j + "--==--" + k);
				  }
				  }}}
		 
		 for (int i = 0; i <= 3; i++) {
			  for (int j = 1; j <= 5; j++) {
				 // for (int k = 1; k <= 5; k++)  
					  
					  
					  MSData[ID][i][j][2] = MSData[ID][i][j][1] - MSData[ID][i][j][0];
					  System.out.println(ID + "pspsp" + MSData[ID][j][2]);
					  
					  }
		 
		 
		 
		 }
	  
	  
	  }
	  
	  
	  
	  
	  if (getSuperstep() > 1 && getSuperstep() < 7) 
	  {
		  
		  if (getSuperstep() == 2)
			  Uptack[ID] = false;
		  
		  //double bid = MSData[MSctr[ID]][ID][2];

		  
		  //System.out.println(ID + "  says Uptack is " + Uptack[ID] + " bid " + bid+" my ctr is "+MSctr[ID]);
		  
		  
		  for (int i = 1; i <= MSctr[ID];i++)
		  {
			  double bid = MSData[ID][i][1][2];
			  //System.out.println(ID + "  says Uptack is " + Uptack[ID] + " bid " + bid+" my ctr is "+MSctr[ID]);
			  
		  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

			  System.out.println(ID + " bid "+ edge.getTargetVertexId());
			  sendMessage(edge.getTargetVertexId(), new DoubleWritable(bid));
				
			  }
		  }
		
		  
	  for (DoubleWritable message : messages) {
	      
	      double msg = message.get();
	      boolean TmpUT = true;
	      
	      if (Uptack[ID]==false)
	      {
	      for ( int i = 1; i <= MSctr[ID]; i++)
	      {
	    	  
	    	  if ((int)msg == MSData[ID][i][1][2])
	    	  {
	    		  
	    		  TmpUT = false;
	    		  
	    		  //System.out.println(ID + " find uptack is "+ Uptack[ID]);
	    		  
	    		  
	    	  }
	      
	      }
	      }
	    	  if (TmpUT)
	    		  Uptack[ID] = true;
	    	  
	    	  //if ( i == MSctr[ID])
	    		  System.out.println(ID + " uptack is "+ Uptack[ID]);
	      
	  }
	  
	  }
	  
	  
		  
	  
	
	  
	  
	  
	  
	 if (getSuperstep() > 10 && getSuperstep() < 19) 
	  {
		  
		  double cap =  vertex.getValue().get();
		  
		  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

			  //System.out.println(ID + " says its capacity is "+ cap);
			  sendMessage(edge.getTargetVertexId(), new DoubleWritable(cap));
			  sendMessage(edge.getTargetVertexId(), new DoubleWritable(ID));
				
		 }
		  FogNCap[ID][ID]=(int)cap;
		  
		  boolean first = true;
		  double capcom = -1;
		  
		  for (DoubleWritable message : messages) {
		      
			  if (first)
			  {
				  capcom = message.get();
				  first = false;
			  }
			  else
			  {
			  for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {

				  //System.out.println(ID+" says capacity of " + message + " is " + capcom);
				  sendMessage(edge.getTargetVertexId(), new DoubleWritable(capcom));
				  sendMessage(edge.getTargetVertexId(), new DoubleWritable(message.get()));
				  
				  FogNCap[ID][(int)message.get()]=(int)capcom;
					
			 }
			  first = true;
			  }
		  }
		  
		 
		  
		 
	  
	  }
	  
	  if (getSuperstep() == 19)
	  {
		  
		  if (FogNCap[ID][ID] != vertex.getValue().get())
		  {
			  Uptack[ID] = true;
			  System.out.println(ID + " UPTACK !!!! ");
		  }
		  
	  }
	  
	  
	  if (getSuperstep() > 19 && getSuperstep() < 30)
	  {
		  
		  int WinnerApp = -1;
		  int WinnerTime = 1000;
		  int WinnerMS = 0;
		  //int AccTime[] = new int[MSctr[ID]];
		  
		  System.out.println(ID+" says SuperStep " + getSuperstep());
		  
		  
		  
		  
		  for (int i = 1; i <= MSctr[ID]; i++)
		  {
			
			  
			  
			
			  if(MSData[ID][i][1][2] < WinnerTime)
			{
				  
				  
				WinnerTime = MSData[ID][i][1][2];
				MSData[ID][i][1][4] = 1;
			    WinnerApp = i;
			    WinnerMS = 1;
			    //System.out.println(ID+" says MS " + WinnerApp + " isssss winner with time " + WinnerTime);
			}
			  
			if (WinnerApp!=-1)
			{
				
			MSData[ID][WinnerApp][1][4] = 1;
			}
		  }
		  
		  
		  
		  
		  
		  
		  if (WinnerApp == -1)
		  {
			  //System.out.println(ID+" says Hello ");
			  for (int i = 1; i <= MSctr[ID]; i++)
			  {
				  
				  //AccTime[i] = 0;

						for (int j = 2; j <= 10; j++)
						  {
							
							if ( MS[i][1][j] == 1 )
							{
								if(MSData[ID][i][j][2] < WinnerTime)
								{
									WinnerTime = MSData[ID][i][j][2];
								    WinnerApp = i;
								    WinnerMS = j;
								   
								    //System.out.println(ID+" says MS " + WinnerApp + " isssss winner with time " + WinnerTime);
								}
								
								
								//System.out.println(ID+" says in Acc Time " + WinnerTime);
							}
						  }
						

				}
			  
			  
			  
			  
			  /*for (int i = 1; i <= MSctr[ID]; i++)
			  {
			  for (int j = 2; j <= 10; j++)
			  		{	
				  		if ( MS[i][1][j] == 1 )
				  			{
				  				MSData[ID][WinnerApp][j][4] = 1;
				  			}
			  		}
			  }*/
			  
			  
			  
			  
			  }
		  
		  
		  
		  int WinnerNode = -1;
		  int WinnerCap = 0;
		  int NewVal = 0;
		  
		  for (int i = 0; i < N; i++)
		  {
			  
			if(FogNCap[ID][i] > WinnerCap)
			{
				WinnerCap = FogNCap[ID][i];
			    WinnerNode = i;
			    //System.out.println(ID+" says MS " + WinnerApp + " isssss winner with time " + WinnerTime);
			}
			
		  }
		  
		 /* int CapAcc = 0;
		  
		  for (int i = 1; i <= 10; i++)
		  {
			  if (MSData[ID][WinnerApp][i][4] == 1)
			  {	  
				  CapAcc +=  MSData[ID][WinnerApp][i][3];
				  //System.out.println(ID+" says MS " + i + " from App  " + WinnerApp + " is allocated to Fog Node " + WinnerNode );
			  }
		  }*/
		  
		  
		  
		  
		  
		  if ( MSData[ID][WinnerApp][WinnerMS][3] < WinnerCap)
		  {
		  
			  //for (int i = 1; i <= 10; i++)
			  //{
				//  if (MSData[ID][WinnerApp][i][4] == 1)
				  //{	  
					  System.out.println(ID+" says says MS " + WinnerMS + " from App " + WinnerApp + " is winner with time " + WinnerTime + " and Capacity Need " + MSData[ID][WinnerApp][WinnerMS][3]);
					  System.out.println(ID+" says FNode " + WinnerNode + " is winner with capacity " + WinnerCap);
					  //System.out.println(ID+" says MS " + i + " from App  " + WinnerApp + " is allocated to Fog Node " + WinnerNode );
				  //}
			  //}
		  
		  
		  
		  //int cap = WinnerCap;
		  //for (int i = 1; i <= 10; i++)
		  //{
			  
			  
			//  if (MSData[ID][WinnerApp][i][4] == 1)
			  //{	  
				  //cap -= MSData[ID][WinnerApp][i][3];
				  
				  MSData[ID][WinnerApp][WinnerMS][4] = 2;
				  MSData[ID][WinnerApp][WinnerMS][2] = 1000;
			  //}
			  
			  //System.out.println(ID+" says cap : " + cap);
			  NewVal = WinnerCap - MSData[ID][WinnerApp][WinnerMS][3];
		  
		  
		  
		  
		  if (ID == WinnerNode)
		  {			  
			  DoubleWritable DVal = new DoubleWritable(NewVal);
			  vertex.setValue(DVal);
			
		  }
		  
			  FogNCap[ID][WinnerNode]= NewVal;
			  
			  
			  
		  } 
		  else
		  {
			  System.out.println(ID+" says No Capacity No winner");
		  }
			  
 
		// vertex.voteToHalt();
  }
	  
	  
	  
	  
	  
	  
	  
	  if (getSuperstep() == 30)
	  {
		  
		  if (Uptack[ID] = true)
		  {
			  System.out.println(ID + " UPTACK !!!! ");
		  }

		  vertex.voteToHalt();
	  }
	  
	    
    }
}
