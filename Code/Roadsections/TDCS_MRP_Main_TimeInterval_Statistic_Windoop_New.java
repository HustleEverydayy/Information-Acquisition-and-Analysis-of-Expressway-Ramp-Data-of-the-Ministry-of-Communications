/* jdwang@asia.edu.tw 2017.2.23
 * For Windoop Environment 
 * HADOOP_HOME ${eclipse_home}\..\hadoop
 * PATH %PATH%;${eclipse_home}\..\hadoop\bin
 */
package jdwang.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class TDCS_MRP_Main_TimeInterval_Statistic_Windoop_New {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		boolean Windoop = true;
		//boolean Windoop = false;
		//*
		if(Windoop)
		{
			//* For OS: Ms-Windows
			conf.set("fs.defaultFS", "hdfs://0.0.0.0:9000");
			conf.set("mapreduce.framework.name", "yarn");
			//conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
			//conf.set("yarn.resourcemanager.hostname", "0.0.0.0");
			//conf.set("yarn.nodemanager.hostname", "0.0.0.0");		
		} else {
			///* For OS: Linux
			/*
	    	conf.set("mapreduce.task.timeout","50000000");  		
	  		conf.set("mapreduce.map.memory.mb","10240");
	  		//Larger resource limit for reduces.
	  		conf.set("mapreduce.reduce.memory.mb","51200");
	  		//Larger heap-size for child jvms of map
	  		conf.set("mapreduce.map.java.opts","-Xmx8192m");
	  		//Larger heap-size for child jvms of reduces.
	  		conf.set("mapreduce.reduce.java.opts","-Xmx40960m");
	  		// The total amount of buffer memory to use while sorting files, in megabytes. 
	  		//conf.set("mapreduce.task.io.sort.mb","4095");
	  		//conf.set("mapreduce.task.io.sort.mb","2047");
	  		conf.set("mapreduce.task.io.sort.mb","1792");
	  		conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.1");
	  		conf.set("mapreduce.job.reduce.slowstart.completedmaps","1.0");
	  		*/
		}
		
		Job job = Job.getInstance(conf, "TDCS_MRP_Main_TimeInterval_Statistic_Windoop");
		job.setJarByClass(TDCS_MRP_Main_TimeInterval_Statistic_Windoop_New.class);
		// TODO: specify a mapper
		job.setMapperClass(TDCS_MRP_SpecificTrip_Mapper_KeyAlone_New.class);
		
		///*
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	   // */
		
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
	  	
	  	FileSystem hdfs = FileSystem.get(conf);
	  	System.out.println("Working Directory -> " + hdfs.getWorkingDirectory().toString());
	  	
	  	// 你只需要修改輸入目錄
	  	
	  	String inputDir = "TDCS_CandidateMR_2018_9_2018_9_Date-Weekday-Vehicle_M1_TF2_CF1_Length1_MRP";
	  	
	  	String OutputDir = inputDir+ "_Output";
	    
	  	Path inputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+inputDir);
	    Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+OutputDir);
		
	   
	    FileInputFormat.addInputPath(job, inputPath);
	    
		FileOutputFormat.setOutputPath(job, outputPath);
			
		if(hdfs.exists(outputPath)) {
	  		hdfs.delete(outputPath, true);
	  	}
		
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
