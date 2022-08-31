
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//public class TDCS_MRP_Reducer extends Reducer<Text, Text, Text, Text> {
//public class TDCS_MRP_TimeInterval_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
public class TDCS_MRP_TimeInterval_Reducer_8 extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();
	//public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    //public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
    	/*
    	00##13##-3-3-2-2-1-1-1	2##2##8##(2018-01-23_Tue_31#1#1)(2018-07-20_Fri_31#1#1)
    	00##13##-3-3-2-2-1-1-1	2##2##8##(2018-01-01_Mon_31#1#1)(2018-12-24_Mon_31#1#1)
    	00##13##-3-3-2-2-1-1-1	2##2##8##(2017-07-24_Mon_31#1#1)(2018-04-01_Sun_31#1#1)
    	00##13##-3-3-2-2-1-1-1	4##4##8##(2017-03-22_Wed_31#1#1)(2018-05-28_Mon_31#1#1)(2018-05-28_Mon_32#1#1)(2019-05-30_Thu_31#1#1)
    	00##13##-3-3-2-2-1-1-1	3##3##8##(2017-08-05_Sat_31#1#1)(2017-12-30_Sat_31#1#1)(2018-11-09_Fri_31#1#1)
    	00##13##-3-3-2-2-1-1-1	2##2##8##(2019-02-25_Mon_31#1#1)(2019-08-26_Mon_31#1#1)
    	*/
    	
    	int Total_TF = 0;
    	int Total_CF = 0;
    	int Length = 0;
    	String FreqDistribution = "";
    	
	    for (Text val : values) {
	    		// TF+"##"+DF+"##"+Length+"##"+Date_Weekday_VehicleTypes_FreqDistribution
	    	//String [] Info  = val.toString().split("##");
			// context.write(key, val);
	    	String [] Info  = val.toString().split("##");
	    	Total_TF = Total_TF + Integer.valueOf(Info[0]);
	    	Total_CF = Total_CF + Integer.valueOf(Info[1]);
	    	Length = Integer.valueOf(Info[2]);
	    	FreqDistribution = FreqDistribution + Info[3];
    	}
	    Text Value = new Text();
	    
	    String Temp = Integer.toString(Total_TF)+"##"+Integer.toString(Total_CF)+"##"+Integer.toString(Length)+"##"+FreqDistribution;
		Value.set(Temp);
	        
	    context.write(key, Value);
	} //End of public void reduce
} // End of public class TDCS_MRP_Reducer extends Reducer
