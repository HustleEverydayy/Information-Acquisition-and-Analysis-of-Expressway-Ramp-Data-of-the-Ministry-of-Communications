/* jdwang@asia.edu.tw 2017.2.23
 * For Windoop Environment 
 * HADOOP_HOME ${eclipse_home}\..\hadoop
 * PATH %PATH%;${eclipse_home}\..\hadoop\bin
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TDCS_MRP_Main_TimeInterval_Statistic_Windoop {
	public static void main(String[] args) throws Exception {
		
boolean useResourceManager = true;
		
		//Step 0. Set configuration
		Configuration conf = new Configuration();	
		
		if(useResourceManager)
		{
			conf.set("fs.defaultFS", "hdfs://0.0.0.0:9000");
			conf.set("mapreduce.framework.name", "yarn");
			//conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
			//conf.set("yarn.resourcemanager.hostname", "0.0.0.0");
			//conf.set("yarn.nodemanager.hostname", "0.0.0.0");		
		}
		else
		{
			conf.set("fs.defaultFS", "file:///");
			conf.set("mapreduce.framework.name", "local");
			//conf.set("yarn.resourcemanager.address", "localhost");
			conf.set("hadoop.tmp.dir", "/windoop/tmp/hadoop-${user.name}");
		}
	
		/*
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: TDCS_MRP_Main_TimeInterval_Statistic_Windoop -DStartQantryID=? -DEndQantryID=? -DLengthQantryID=? <input> <output>");
	      System.exit(2);
	    }
		*/
		
		Job job = Job.getInstance(conf, "TDCS_MRP_Main_TimeInterval_Statistic_Windoop");
		
		job.setJarByClass(TDCS_MRP_Main_TimeInterval_Statistic_Windoop.class);
		// TODO: specify a mapper
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_1_TotalPassTime_SpecifiedSegment.class);
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_2_TotalPassTime_SpecifiedSegment_VehicleType.class);
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_3_TotalPassTime_SpecifiedSegment_VehicleType_Weekday.class);
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_4_TotalPassTime_SpecifiedSegment_CostTimeIntervals.class);
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_5_SpecifiedSegment_DateWeekday_Hour_VT_TotalTime_TraveralTimeIntervals_TF.class);		
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_6_SpecifiedSegment_TotalPassTime__TravelTimeIntervals_ClassFrequencyDistribution.class);		
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_7_SpecifiedSegment_Hour_TotalPassTime__TravelTimeIntervals_ClassFrequencyDistribution.class);		
		job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_8.class);		
					
		
		//job.setMapperClass(TDCS_MRP_TimeInterval_Mapper_1_TotalPassTime.class);
						
		//job.setMapOutputKeyClass(Text.class);    
	    //job.setMapOutputValueClass(Text.class); 
		
		// TODO: specify a reducer
		
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer.class);
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_3_TotalPassTime_VehicleType_Weekday.class);
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_4_TotalPassTime_PassTimeIntervals.class);
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_4_VTs_TF.class);
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_5_DateWeekday_Hour_VT_TraveralTimeIntervals_TF.class);
		//job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_6_VTs_TF.class);
		job.setReducerClass(TDCS_MRP_TimeInterval_Reducer_8.class);
		
		//job.setNumReduceTasks(10);   //reduce

		// TODO: specify output types
		///*
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//*/
		/*
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		*/
		/*
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		*/ 
		/*
		FileSystem hdfs = FileSystem.get(conf);
	  	System.out.println("Working Directory -> " + hdfs.getWorkingDirectory().toString());
	  	//System.out.println("Working Directory -> " + hdfs.default.name);
	  	
	    LocalFileSystem local = FileSystem.getLocal(conf);   
	    //Path localPath = new Path(local.getWorkingDirectory().toString() + "/"+otherArgs[0]);
	    Path inputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+otherArgs[0]);
	    Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+otherArgs[1]); 
	    */
		
		// TODO: specify input and output DIRECTORIES (not files)
		
		
		//FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F1860S_03F2100S_G6"));		
		FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F1779S_03F2129S_G8"));
		//FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F2125N_03F1779N_G8"));		
		//FileInputFormat.setInputPaths(job, new Path("Input_Test"));		
		//FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F1739S_03F2152S_G10"));
		//FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F1739S_03F2152S_G10"));
		
		
		//FileInputFormat.setInputPaths(job, new Path("TDCS_CandidateMR_2016_11_2019_10_03F2100N_03F1860N_G6"));				
		FileSystem hdfs = FileSystem.get(conf);
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_CandidateMR_2016_11_2019_10_03F1860S_03F2100S_G6_GantryID_TotalTimeVsFrequency_VehicleType_Weekday");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_CandidateMR_2016_11_2019_10_03F2100N_03F1860N_G6_GantryID_TotalTimeVsFrequency_VehicleType_Weekday");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_CandidateMR_2016_11_2019_10_03F2100N_03F1860N_G6_GantryID_TotalTimeVsFrequency_CostTimeIntervals");
		
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_MR_201611_201910_03F1779S_03F2129S_G8_NonHour_Key_TotalTime_TimeIntervals_Value_TFCFLength_ClassFrequency");
		Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_MR_201611_201910_03F1779S_03F2129S_G8_Key_Hour_TotalTime_TimeIntervals_Value_TFCFLength_ClassFrequency");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_2016_11_2019_10_03F2125N_03F1779N_G8_Key_Hour_TotalTime_TimeIntervals_Value_TFCFLength_ClassFrequency");
		
		
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_MR_201611_201910_03F1779S_03F2129S_G8_Date_Weekday_Hour_TimeIntervals_TotalTime_VTsFrequency");		
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_MR_201611_201910_03F1779S_03F2129S_G8_Date_Weekday_TimeIntervals_TotalTime_VTsFrequency");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_TDCS_MR_201611_201910_03F1739S_03F2152S_G10_Key_TotalTimeTimeIntervals_Value_TFCFLength");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+"Output_Test");
		
	    //FileInputFormat.addInputPath(job, inputPath); 
		FileOutputFormat.setOutputPath(job, outputPath);
			
		if(hdfs.exists(outputPath)) {
	  		hdfs.delete(outputPath, true);
	  	}
		/*
		if (!job.waitForCompletion(true))
			return;
			*/
		if(job.waitForCompletion(true))
	  	{
	  		System.out.println("Job Done!");
	  		System.exit(0);
	  	}
	  	else
	  	{
	  		System.out.println("Job Failed!");
	  		System.exit(1);
	  	}    
	}

}
