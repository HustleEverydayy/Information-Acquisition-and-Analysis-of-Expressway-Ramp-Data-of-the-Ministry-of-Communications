package jdwang.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.NullWritable;

//public class TDCS_MRP_Mapper extends Mapper<LongWritable, Text, Text, Text> {
//public class TDCS_MRP_SpecificTrip_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//public class TDCS_MRP_SpecificTrip_Mapper_KeyAlone extends Mapper<LongWritable, Text, Text, NullWritable> {
public class TDCS_MRP_SpecificTrip_Mapper_KeyAlone_New extends Mapper<LongWritable, Text, Text, Text> {
	//05F0001N_00_M1_05 03F0201S_00_M1_07 03F0217S_00_M1_08 03F0301S_00_M1_13##5##5##4
	//private String StartQantryID = "05F0528N"; 
	//private String EndQantryID = "05F0001N";
	
	// 01F0005N_11_M1_01 06G0163E_11_M1_12 06G0173E_11_M1_13	
	//4##4##3##3##(2019-09-26_Thu_31#2#2)(2019-09-26_Thu_32#1#1)(2019-10-04_Fri_32#1#1)
	
	private String StartQantryID = "03F2125N"; 
	private String EndQantryID = "03F1779N";
	private int LengthQantryID = 8;
		
	//https://stackoverflow.com/questions/8244474/passing-arguments-to-hadoop-mappers
	//https://vimsky.com/zh-tw/examples/detail/java-method-org.apache.hadoop.mapreduce.Mapper.Context.getConfiguration.html
	
	
	
	private final static IntWritable one = new IntWritable(1);
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
				
		/*
		 * 01F0005N_15_M1_56 06G0163E_16_M1_06 06G0173E_16_M1_07 06G0239E_16_M1_12	
		 * 2##2##2##4##
		 * (2019-09-27_Fri_31#1#1)(2019-10-10_Thu_31#1#1)
		 */
		
		//01F0005N_11_M1_01 06G0163E_11_M1_12 06G0173E_11_M1_13	
		//4##4##3##3##(2019-09-26_Thu_31#2#2)(2019-09-26_Thu_32#1#1)(2019-10-04_Fri_32#1#1)
		
		
		
		String line = ivalue.toString();
    	//String[] Items = line.split("\t");
		String[] Items = line.split("##");
    	
    	String QantryID = Items[0];
    	    	
    	
    	String[] QantryIDInfo = QantryID.split(" ");
    	
    	
    	String FirstQantryID = QantryIDInfo[0].substring(0,8);
    	//String FirstQantryID_Hour = QantryIDInfo[0].substring(9,11);
    	String LastQantryID = QantryIDInfo[QantryIDInfo.length-1].substring(0,8);
    	
    	
    	if ((FirstQantryID.equals(StartQantryID))
  	    	  &&(LastQantryID.equals(EndQantryID))
  	    	  &&(LengthQantryID == QantryIDInfo.length)) { 	
    		Text PatternKey = new Text();
    		Text ValueKey = new Text();
    		
    		PatternKey.set(Items[0]);
    		
    		String Temp = Items[1];
    		for (int i=2;i<Items.length;++i){
    			Temp = Temp+"##"+Items[i];
    		}
    		ValueKey.set(Temp);
 		
    		context.write(PatternKey,ValueKey); 		
    	}
    	
	} //End of public void map
} // End of public class TDCS_MRP_Mapper
