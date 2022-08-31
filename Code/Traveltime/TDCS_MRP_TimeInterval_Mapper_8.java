
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//public class TDCS_MRP_Mapper extends Mapper<LongWritable, Text, Text, Text> {
//public class TDCS_MRP_TimeInterval_Mapper_1_TotalPassTime extends Mapper<LongWritable, Text, Text, IntWritable> {
//public class TDCS_MRP_TimeInterval_Mapper_4_TotalPassTime_SpecifiedSegment_CostTimeIntervals extends Mapper<LongWritable, Text, Text, Text> {
public class TDCS_MRP_TimeInterval_Mapper_8 extends Mapper<LongWritable, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);		
  	
	/* 
	private String StartQantryID = "03F2100N"; 
	private String EndQantryID = "03F1860N";
	*/
	/* S G6
	private String StartQantryID = "03F1860S"; 
	private String EndQantryID = "03F2100S";
	private int LengthQantryID = 6;
	*/
	/* S G8
	private String StartQantryID = "03F1779S"; 
	private String EndQantryID = "03F2129S";
	private int LengthQantryID = 8;
	*/
	
	///* N G8
	private String StartQantryID = "03F2125N"; 
	private String EndQantryID = "03F1779N";
	private int LengthQantryID = 8;
	//*/
	
	
	/* S G10
	private String StartQantryID = "03F1739S"; 
	private String EndQantryID = "03F2152S";
	private int LengthQantryID = 10;
	*/
	
	/* S G12
	private String StartQantryID = "03F1710S"; 
	private String EndQantryID = "03F2194S";
	private int LengthQantryID = 12;
	*/
	
	/*
	private String StartQantryID = ""; 
	private String EndQantryID = "";
	private int LengthQantryID = 0;
	
  	//https://stackoverflow.com/questions/8244474/passing-arguments-to-hadoop-mappers
	//https://vimsky.com/zh-tw/examples/detail/java-method-org.apache.hadoop.mapreduce.Mapper.Context.getConfiguration.html
  	public void setup(Context context)
  	{
	    	Configuration conf = context.getConfiguration();
	    	StartQantryID = conf.get("StartQantryID", "03F1860S");
	    	EndQantryID = conf.get("EndQantryID", "03F2100S");
	    	LengthQantryID = conf.getInt("LengthQantryID", 6);
  	} 
  	*/
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		/* input format 
		 * (03F1860S_11_M1_27)(GateID)(Direction:N or S)_Hour_M(TimeUnit)_TimeUnitIndex
		 * 626##626##484##6## : ##TF##DF##CF##PatternLength
		 * (2016-11-04_Fri_31#1#1) : (Date_WeekDay_VehicleType##TF##DF)
		03F1860S_11_M1_27 03F1944S_11_M1_31 03F1991S_11_M1_33 03F2066S_11_M1_37 03F2079S_11_M1_38 03F2100S_11_M1_39	626##626##484##6##(2016-11-04_Fri_31#1#1)(2016-11-06_Sun_32#2#2)(2016-11-09_Wed_31#1#1)(2016-11-13_Sun_31#2#2)(2016-11-14_Mon_31#1#1)(2016-11-18_Fri_31#1#1)(2016-11-24_Thu_31#1#1)(2016-11-25_Fri_31#1#1)(2016-11-28_Mon_31#3#3)(2016-11-29_Tue_31#2#2)(2016-11-30_Wed_31#1#1)(2016-11-30_Wed_32#1#1)(2016-12-01_Thu_31#1#1)(2016-12-04_Sun_31#2#2)(2016-12-06_Tue_31#2#2)(2016-12-08_Thu_31#1#1)(2016-12-09_Fri_31#2#2)(2016-12-12_Mon_31#4#4)(2016-12-13_Tue_31#1#1)(2016-12-14_Wed_31#1#1)(2016-12-17_Sat_31#1#1)(2016-12-18_Sun_31#1#1)(2016-12-20_Tue_31#2#2)(2016-12-20_Tue_32#1#1)(2016-12-22_Thu_31#3#3)(2016-12-22_Thu_32#1#1)(2016-12-23_Fri_31#1#1)(2016-12-25_Sun_31#1#1)(2016-12-29_Thu_31#1#1)(2017-01-02_Mon_32#1#1)(2017-01-03_Tue_31#1#1)(2017-01-05_Thu_31#1#1)(2017-01-05_Thu_32#1#1)(2017-01-06_Fri_31#2#2)(2017-01-08_Sun_32#1#1)(2017-01-09_Mon_31#1#1)(2017-01-10_Tue_31#2#2)(2017-01-13_Fri_31#1#1)(2017-01-15_Sun_31#1#1)(2017-01-18_Wed_31#1#1)(2017-01-18_Wed_32#1#1)(2017-01-19_Thu_31#1#1)(2017-01-23_Mon_32#1#1)(2017-01-25_Wed_31#1#1)(2017-02-01_Wed_31#1#1)(2017-02-05_Sun_31#5#5)(2017-02-10_Fri_31#2#2)(2017-02-16_Thu_31#1#1)(2017-02-17_Fri_31#3#3)(2017-02-18_Sat_31#1#1)(2017-02-28_Tue_31#1#1)(2017-02-28_Tue_32#1#1)(2017-03-01_Wed_31#1#1)(2017-03-01_Wed_32#2#2)(2017-03-04_Sat_31#1#1)(2017-03-05_Sun_31#3#3)(2017-03-06_Mon_31#1#1)(2017-03-06_Mon_32#1#1)(2017-03-11_Sat_31#2#2)(2017-03-12_Sun_31#1#1)(2017-03-13_Mon_31#1#1)(2017-03-14_Tue_31#1#1)(2017-03-17_Fri_31#1#1)(2017-03-19_Sun_31#4#4)(2017-03-20_Mon_31#1#1)(2017-03-22_Wed_31#1#1)(2017-03-24_Fri_31#1#1)(2017-03-26_Sun_31#1#1)(2017-03-27_Mon_31#1#1)(2017-03-29_Wed_32#1#1)(2017-03-30_Thu_31#1#1)(2017-03-31_Fri_31#1#1)(2017-04-03_Mon_31#1#1)(2017-04-03_Mon_32#1#1)(2017-04-09_Sun_31#2#2)(2017-04-12_Wed_31#1#1)(2017-04-13_Thu_31#1#1)(2017-04-15_Sat_31#1#1)(2017-04-15_Sat_32#1#1)(2017-04-16_Sun_32#1#1)(2017-04-18_Tue_31#1#1)(2017-04-23_Sun_31#4#4)(2017-04-23_Sun_32#1#1)(2017-04-24_Mon_32#1#1)(2017-04-27_Thu_31#1#1)(2017-04-30_Sun_31#1#1)(2017-05-03_Wed_32#1#1)(2017-05-11_Thu_31#1#1)(2017-05-14_Sun_31#1#1)(2017-05-15_Mon_31#2#2)(2017-05-16_Tue_31#1#1)(2017-05-18_Thu_31#1#1)(2017-05-19_Fri_31#1#1)(2017-05-21_Sun_31#2#2)(2017-05-21_Sun_32#1#1)(2017-05-26_Fri_31#1#1)(2017-06-01_Thu_31#1#1)(2017-06-06_Tue_31#1#1)(2017-06-07_Wed_31#1#1)(2017-06-11_Sun_31#1#1)(2017-06-19_Mon_32#1#1)(2017-06-26_Mon_31#1#1)(2017-06-30_Fri_31#1#1)(2017-07-04_Tue_31#1#1)(2017-07-06_Thu_31#1#1)(2017-07-08_Sat_31#1#1)(2017-07-09_Sun_31#1#1)(2017-07-13_Thu_31#1#1)(2017-07-18_Tue_31#1#1)(2017-07-20_Thu_31#2#2)(2017-07-21_Fri_31#1#1)(2017-07-23_Sun_31#1#1)(2017-07-24_Mon_32#1#1)(2017-07-25_Tue_31#1#1)(2017-07-27_Thu_32#1#1)(2017-07-28_Fri_31#1#1)(2017-07-29_Sat_32#1#1)(2017-08-04_Fri_31#1#1)(2017-08-06_Sun_31#1#1)(2017-08-14_Mon_31#1#1)(2017-08-16_Wed_32#1#1)(2017-08-17_Thu_31#1#1)(2017-08-24_Thu_31#1#1)(2017-08-25_Fri_31#1#1)(2017-08-27_Sun_31#1#1)(2017-09-03_Sun_31#1#1)(2017-09-04_Mon_31#2#2)(2017-09-05_Tue_32#1#1)(2017-09-10_Sun_31#1#1)(2017-09-14_Thu_31#1#1)(2017-09-15_Fri_31#1#1)(2017-09-16_Sat_31#2#2)(2017-09-16_Sat_32#1#1)(2017-09-17_Sun_31#1#1)(2017-09-18_Mon_31#1#1)(2017-09-20_Wed_31#1#1)(2017-09-20_Wed_32#1#1)(2017-09-21_Thu_31#1#1)(2017-09-22_Fri_31#1#1)(2017-09-24_Sun_31#1#1)(2017-09-27_Wed_31#1#1)(2017-10-01_Sun_31#1#1)(2017-10-02_Mon_31#1#1)(2017-10-04_Wed_31#1#1)(2017-10-05_Thu_32#1#1)(2017-10-10_Tue_31#1#1)(2017-10-10_Tue_32#1#1)(2017-10-11_Wed_31#1#1)(2017-10-12_Thu_31#1#1)(2017-10-17_Tue_31#2#2)(2017-10-20_Fri_31#3#3)(2017-10-24_Tue_31#2#2)(2017-10-25_Wed_32#1#1)(2017-10-26_Thu_31#1#1)(2017-10-28_Sat_31#3#3)(2017-10-29_Sun_31#1#1)(2017-11-01_Wed_31#2#2)(2017-11-01_Wed_32#1#1)(2017-11-05_Sun_31#2#2)(2017-11-07_Tue_32#1#1)(2017-11-14_Tue_31#2#2)(2017-11-15_Wed_32#1#1)(2017-11-16_Thu_31#1#1)(2017-11-17_Fri_31#1#1)(2017-11-18_Sat_32#1#1)(2017-11-22_Wed_31#3#3)(2017-11-23_Thu_31#1#1)(2017-11-24_Fri_31#2#2)(2017-11-26_Sun_31#1#1)(2017-11-28_Tue_31#1#1)(2017-11-29_Wed_31#1#1)(2017-11-30_Thu_31#1#1)(2017-12-02_Sat_31#1#1)(2017-12-02_Sat_32#1#1)(2017-12-03_Sun_31#2#2)(2017-12-04_Mon_31#1#1)(2017-12-06_Wed_32#2#2)(2017-12-07_Thu_31#2#2)(2017-12-08_Fri_31#1#1)(2017-12-09_Sat_31#1#1)(2017-12-09_Sat_32#1#1)(2017-12-11_Mon_31#1#1)(2017-12-12_Tue_31#1#1)(2017-12-14_Thu_31#1#1)(2017-12-17_Sun_32#1#1)(2017-12-18_Mon_31#1#1)(2017-12-18_Mon_32#1#1)(2017-12-19_Tue_31#1#1)(2017-12-20_Wed_31#1#1)(2017-12-24_Sun_31#1#1)(2017-12-24_Sun_32#1#1)(2017-12-26_Tue_31#1#1)(2017-12-27_Wed_31#1#1)(2017-12-28_Thu_32#1#1)(2018-01-01_Mon_31#1#1)(2018-01-02_Tue_31#1#1)(2018-01-05_Fri_31#1#1)(2018-01-07_Sun_31#1#1)(2018-01-11_Thu_31#1#1)(2018-01-13_Sat_31#1#1)(2018-01-14_Sun_31#1#1)(2018-01-14_Sun_32#1#1)(2018-01-16_Tue_31#2#2)(2018-01-17_Wed_31#2#2)(2018-01-18_Thu_31#1#1)(2018-01-23_Tue_32#1#1)(2018-01-24_Wed_31#1#1)(2018-02-01_Thu_32#1#1)(2018-02-02_Fri_31#1#1)(2018-02-08_Thu_31#2#2)(2018-02-09_Fri_31#2#2)(2018-02-09_Fri_42#1#1)(2018-02-11_Sun_31#1#1)(2018-02-12_Mon_31#3#3)(2018-02-13_Tue_32#1#1)(2018-02-20_Tue_31#1#1)(2018-02-23_Fri_31#1#1)(2018-02-25_Sun_31#2#2)(2018-02-26_Mon_31#1#1)(2018-02-26_Mon_32#1#1)(2018-03-05_Mon_31#2#2)(2018-03-07_Wed_31#1#1)(2018-03-09_Fri_31#1#1)(2018-03-10_Sat_32#1#1)(2018-03-14_Wed_31#1#1)(2018-03-14_Wed_32#2#2)(2018-03-16_Fri_31#1#1)(2018-03-18_Sun_32#1#1)(2018-03-20_Tue_32#1#1)(2018-03-22_Thu_31#1#1)(2018-03-26_Mon_31#1#1)(2018-03-30_Fri_31#1#1)(2018-04-03_Tue_31#1#1)(2018-04-05_Thu_31#2#2)(2018-04-05_Thu_32#1#1)(2018-04-07_Sat_31#1#1)(2018-04-09_Mon_31#1#1)(2018-04-10_Tue_31#1#1)(2018-04-10_Tue_32#1#1)(2018-04-11_Wed_31#1#1)(2018-04-12_Thu_31#2#2)(2018-04-13_Fri_31#1#1)(2018-04-15_Sun_31#2#2)(2018-04-16_Mon_31#2#2)(2018-04-16_Mon_32#1#1)(2018-04-18_Wed_31#1#1)(2018-04-18_Wed_32#1#1)(2018-04-19_Thu_31#1#1)(2018-04-22_Sun_32#1#1)(2018-04-24_Tue_31#1#1)(2018-04-26_Thu_31#3#3)(2018-04-29_Sun_31#2#2)(2018-04-29_Sun_32#1#1)(2018-05-07_Mon_31#1#1)(2018-05-10_Thu_31#1#1)(2018-05-13_Sun_31#2#2)(2018-05-15_Tue_31#1#1)(2018-05-17_Thu_31#1#1)(2018-05-19_Sat_32#1#1)(2018-05-20_Sun_31#1#1)(2018-05-27_Sun_31#2#2)(2018-05-29_Tue_31#1#1)(2018-05-30_Wed_31#1#1)(2018-05-31_Thu_31#1#1)(2018-06-01_Fri_31#1#1)(2018-06-02_Sat_31#1#1)(2018-06-03_Sun_31#2#2)(2018-06-07_Thu_31#1#1)(2018-06-08_Fri_31#1#1)(2018-06-08_Fri_32#1#1)(2018-06-10_Sun_31#2#2)(2018-06-15_Fri_31#1#1)(2018-06-17_Sun_31#2#2)(2018-06-17_Sun_32#1#1)(2018-06-23_Sat_31#1#1)(2018-06-25_Mon_31#1#1)(2018-07-01_Sun_31#2#2)(2018-07-09_Mon_31#1#1)(2018-07-09_Mon_32#1#1)(2018-07-15_Sun_31#2#2)(2018-07-17_Tue_31#1#1)(2018-07-22_Sun_31#1#1)(2018-07-24_Tue_31#1#1)(2018-07-26_Thu_31#2#2)(2018-07-26_Thu_32#1#1)(2018-08-06_Mon_31#3#3)(2018-08-06_Mon_32#1#1)(2018-08-07_Tue_32#1#1)(2018-08-10_Fri_31#1#1)(2018-08-11_Sat_32#1#1)(2018-08-12_Sun_31#1#1)(2018-08-14_Tue_31#1#1)(2018-08-16_Thu_32#1#1)(2018-08-19_Sun_31#2#2)(2018-08-22_Wed_31#1#1)(2018-08-23_Thu_31#1#1)(2018-08-24_Fri_31#3#3)(2018-08-26_Sun_31#2#2)(2018-08-29_Wed_32#1#1)(2018-08-31_Fri_31#1#1)(2018-09-03_Mon_32#1#1)(2018-09-04_Tue_31#2#2)(2018-09-09_Sun_31#1#1)(2018-09-09_Sun_32#1#1)(2018-09-12_Wed_31#1#1)(2018-09-13_Thu_31#1#1)(2018-09-15_Sat_31#1#1)(2018-09-20_Thu_31#1#1)(2018-09-23_Sun_31#1#1)(2018-09-26_Wed_31#3#3)(2018-09-29_Sat_31#1#1)(2018-09-30_Sun_31#1#1)(2018-09-30_Sun_32#1#1)(2018-10-01_Mon_31#1#1)(2018-10-02_Tue_31#1#1)(2018-10-06_Sat_32#1#1)(2018-10-07_Sun_31#2#2)(2018-10-15_Mon_31#1#1)(2018-10-18_Thu_31#3#3)(2018-10-18_Thu_32#1#1)(2018-10-19_Fri_31#1#1)(2018-10-23_Tue_31#1#1)(2018-10-24_Wed_32#1#1)(2018-10-25_Thu_31#2#2)(2018-10-25_Thu_32#1#1)(2018-11-05_Mon_31#1#1)(2018-11-06_Tue_31#1#1)(2018-11-07_Wed_31#2#2)(2018-11-11_Sun_31#3#3)(2018-11-15_Thu_31#1#1)(2018-11-15_Thu_32#2#2)(2018-11-17_Sat_31#2#2)(2018-11-19_Mon_32#1#1)(2018-11-20_Tue_31#2#2)(2018-11-21_Wed_31#1#1)(2018-11-23_Fri_31#1#1)(2018-11-28_Wed_31#1#1)(2018-12-02_Sun_31#1#1)(2018-12-02_Sun_32#1#1)(2018-12-03_Mon_31#1#1)(2018-12-05_Wed_31#1#1)(2018-12-09_Sun_32#1#1)(2018-12-10_Mon_32#1#1)(2018-12-12_Wed_31#2#2)(2018-12-13_Thu_31#1#1)(2018-12-14_Fri_32#1#1)(2018-12-15_Sat_31#1#1)(2018-12-16_Sun_31#1#1)(2018-12-22_Sat_31#1#1)(2018-12-23_Sun_31#3#3)(2018-12-24_Mon_31#1#1)(2018-12-25_Tue_31#2#2)(2018-12-26_Wed_31#2#2)(2018-12-27_Thu_31#1#1)(2018-12-27_Thu_32#1#1)(2018-12-28_Fri_32#1#1)(2019-01-05_Sat_31#1#1)(2019-01-10_Thu_31#3#3)(2019-01-11_Fri_31#1#1)(2019-01-12_Sat_31#2#2)(2019-01-12_Sat_32#1#1)(2019-01-13_Sun_31#1#1)(2019-01-14_Mon_31#3#3)(2019-01-18_Fri_31#1#1)(2019-01-20_Sun_31#1#1)(2019-01-20_Sun_32#1#1)(2019-01-21_Mon_31#1#1)(2019-01-22_Tue_31#1#1)(2019-01-23_Wed_31#1#1)(2019-01-26_Sat_31#5#5)(2019-01-27_Sun_31#1#1)(2019-01-27_Sun_32#1#1)(2019-01-28_Mon_32#1#1)(2019-01-29_Tue_31#1#1)(2019-01-31_Thu_31#1#1)(2019-02-04_Mon_31#2#2)(2019-02-04_Mon_32#1#1)(2019-02-09_Sat_31#2#2)(2019-02-10_Sun_31#1#1)(2019-02-11_Mon_31#2#2)(2019-02-13_Wed_31#1#1)(2019-02-14_Thu_31#1#1)(2019-02-14_Thu_32#1#1)(2019-02-15_Fri_31#1#1)(2019-02-15_Fri_32#1#1)(2019-02-17_Sun_31#1#1)(2019-02-18_Mon_31#3#3)(2019-02-20_Wed_32#1#1)(2019-02-21_Thu_32#1#1)(2019-02-22_Fri_32#1#1)(2019-02-23_Sat_31#1#1)(2019-02-24_Sun_31#1#1)(2019-02-24_Sun_32#1#1)(2019-03-06_Wed_31#3#3)(2019-03-10_Sun_31#1#1)(2019-03-11_Mon_31#3#3)(2019-03-11_Mon_32#2#2)(2019-03-12_Tue_31#3#3)(2019-03-16_Sat_31#2#2)(2019-03-17_Sun_31#1#1)(2019-03-19_Tue_31#1#1)(2019-03-22_Fri_31#1#1)(2019-03-25_Mon_31#1#1)(2019-03-27_Wed_31#1#1)(2019-03-28_Thu_32#1#1)(2019-03-29_Fri_31#1#1)(2019-03-31_Sun_31#3#3)(2019-04-05_Fri_31#4#4)(2019-04-05_Fri_32#1#1)(2019-04-08_Mon_31#1#1)(2019-04-08_Mon_32#1#1)(2019-04-10_Wed_31#1#1)(2019-04-18_Thu_31#1#1)(2019-04-22_Mon_31#2#2)(2019-04-26_Fri_31#1#1)(2019-04-26_Fri_32#1#1)(2019-04-28_Sun_31#1#1)(2019-05-05_Sun_31#2#2)(2019-05-07_Tue_31#1#1)(2019-05-10_Fri_31#1#1)(2019-05-11_Sat_31#1#1)(2019-05-12_Sun_31#1#1)(2019-05-14_Tue_31#1#1)(2019-05-15_Wed_32#1#1)(2019-05-16_Thu_31#1#1)(2019-05-19_Sun_31#1#1)(2019-05-19_Sun_32#1#1)(2019-05-23_Thu_31#1#1)(2019-05-26_Sun_31#2#2)(2019-05-29_Wed_31#2#2)(2019-06-01_Sat_31#1#1)(2019-06-08_Sat_31#1#1)(2019-06-13_Thu_31#1#1)(2019-06-14_Fri_32#1#1)(2019-06-15_Sat_31#1#1)(2019-06-16_Sun_32#1#1)(2019-06-17_Mon_32#2#2)(2019-06-18_Tue_31#1#1)(2019-06-21_Fri_31#1#1)(2019-06-26_Wed_31#2#2)(2019-06-28_Fri_31#3#3)(2019-07-10_Wed_31#1#1)(2019-07-11_Thu_31#1#1)(2019-07-15_Mon_31#1#1)(2019-07-16_Tue_32#1#1)(2019-07-17_Wed_32#1#1)(2019-07-21_Sun_31#1#1)(2019-07-31_Wed_31#1#1)(2019-08-01_Thu_31#2#2)(2019-08-02_Fri_31#1#1)(2019-08-04_Sun_31#2#2)(2019-08-10_Sat_31#1#1)(2019-08-10_Sat_32#1#1)(2019-08-16_Fri_31#1#1)(2019-08-17_Sat_31#1#1)(2019-08-19_Mon_31#2#2)(2019-08-20_Tue_31#1#1)(2019-08-21_Wed_31#1#1)(2019-08-24_Sat_31#1#1)(2019-08-25_Sun_31#1#1)(2019-08-27_Tue_31#1#1)(2019-09-03_Tue_31#1#1)(2019-09-08_Sun_31#1#1)(2019-09-10_Tue_31#1#1)(2019-09-15_Sun_31#1#1)(2019-09-23_Mon_31#1#1)(2019-09-26_Thu_31#1#1)(2019-09-30_Mon_31#1#1)(2019-10-03_Thu_31#1#1)(2019-10-04_Fri_31#1#1)(2019-10-05_Sat_31#1#1)(2019-10-06_Sun_31#1#1)(2019-10-07_Mon_31#1#1)(2019-10-09_Wed_31#1#1)(2019-10-12_Sat_31#1#1)(2019-10-13_Sun_31#1#1)(2019-10-14_Mon_31#1#1)(2019-10-16_Wed_32#1#1)(2019-10-18_Fri_31#1#1)(2019-10-20_Sun_31#3#3)(2019-10-23_Wed_31#1#1)(2019-10-28_Mon_31#1#1)(2019-10-30_Wed_31#1#1)(2019-10-31_Thu_31#1#1)
		03F1860S_11_M1_27 03F1944S_11_M1_31 03F1991S_11_M1_34 03F2066S_11_M1_39 03F2079S_11_M1_40 03F2100S_11_M1_41	56##56##53##6##(2016-12-29_Thu_31#1#1)(2017-02-24_Fri_42#1#1)(2017-03-24_Fri_31#2#2)(2017-04-18_Tue_31#3#3)(2017-05-06_Sat_32#1#1)(2017-06-01_Thu_31#1#1)(2017-06-23_Fri_31#1#1)(2017-06-30_Fri_31#1#1)(2017-07-25_Tue_31#1#1)(2017-08-29_Tue_42#1#1)(2017-10-19_Thu_31#1#1)(2017-10-21_Sat_31#1#1)(2017-10-21_Sat_32#1#1)(2017-10-28_Sat_31#1#1)(2017-11-21_Tue_31#1#1)(2017-12-04_Mon_31#1#1)(2017-12-05_Tue_31#1#1)(2017-12-07_Thu_42#1#1)(2017-12-08_Fri_31#1#1)(2017-12-11_Mon_31#1#1)(2018-01-22_Mon_32#1#1)(2018-01-28_Sun_31#1#1)(2018-03-18_Sun_31#1#1)(2018-03-26_Mon_31#1#1)(2018-03-31_Sat_31#1#1)(2018-04-09_Mon_31#1#1)(2018-07-04_Wed_31#1#1)(2018-07-16_Mon_31#1#1)(2018-07-20_Fri_32#1#1)(2018-09-29_Sat_31#1#1)(2018-10-05_Fri_42#1#1)(2018-10-22_Mon_31#1#1)(2018-12-01_Sat_31#1#1)(2018-12-28_Fri_31#1#1)(2019-01-13_Sun_31#1#1)(2019-01-26_Sat_32#1#1)(2019-02-02_Sat_32#1#1)(2019-02-25_Mon_31#1#1)(2019-03-01_Fri_31#1#1)(2019-03-08_Fri_42#1#1)(2019-03-25_Mon_32#1#1)(2019-04-20_Sat_31#1#1)(2019-05-07_Tue_31#1#1)(2019-05-10_Fri_32#1#1)(2019-05-13_Mon_31#1#1)(2019-05-23_Thu_31#1#1)(2019-05-25_Sat_32#1#1)(2019-06-24_Mon_31#1#1)(2019-08-26_Mon_31#1#1)(2019-09-17_Tue_32#1#1)(2019-09-19_Thu_31#1#1)(2019-10-13_Sun_31#1#1)(2019-10-15_Tue_32#1#1)
		03F1860S_11_M1_27 03F1944S_11_M1_33 03F1991S_11_M1_37 03F2066S_11_M1_43 03F2079S_11_M1_45 03F2100S_11_M1_46	2##2##2##6##(2017-03-30_Thu_32#1#1)(2018-04-09_Mon_5#1#1)
		03F1860S_11_M1_27 03F1944S_11_M1_36 03F1991S_11_M1_39 03F2066S_11_M1_44 03F2079S_11_M1_45 03F2100S_11_M1_47	4##4##4##6##(2017-05-13_Sat_31#1#1)(2018-02-17_Sat_31#1#1)(2018-10-20_Sat_42#1#1)(2019-07-23_Tue_31#1#1)

		*/
		String line = ivalue.toString();
    	String[] Items = line.split("\t");
    	
    	String QantryID_TimeStamp = Items[0]; // Gantry_TimeStamp
    	
    	// 626##626##484##6##
    	
    	String [] Info = Items[1].split("##");
    	
    	String TF = Info[0]; 
    	//String DF = Info[1];
    	String CF = Info[1];
    	String Length = Info[2];
    	String ClassFrequencyDistribution = Info[3];
    		
    	// (2016-11-25_Thu_31#1#1)(2016-12-25_Sat_32#1#1)(2017-01-07_Fri_31#1#1)
    	String [] ClassDistribution = ClassFrequencyDistribution.split("\\)");
    	
    	
    	//TreeMap WeekdayFreqDistribution = new TreeMap <String, Integer>();
    	TreeMap VehicleTypeFreqDistribution = new TreeMap <String, Integer>();
    	TreeMap VehicleType_Weekday_FreqDistribution = new TreeMap <String, Integer>();
    	
    	for (int i=0;i< ClassDistribution.length;++i ){
    		// (2016-11-25_Thu_31#1#1
    		String [] OneClassInfo = ClassDistribution[i].split("#");
    		String OneDate = OneClassInfo[0].substring(1,11);
    		String OneWeekday = OneClassInfo[0].substring(12,15);
    		String OneVehicleType = OneClassInfo[0].substring(16);
    		
    		int OneTF = Integer.valueOf(OneClassInfo[1]);
      		  
    		
    		
    	} // End of for (int i=0;i< ClassDistribution.length;++i )
    	
    	/*
    	 * The information of first GantryID in one MRP
    	 * 03F1860S_11_M1_27 03F1944S_11_M1_33 03F1991S_11_M1_37 03F2066S_11_M1_43 03F2079S_11_M1_45 03F2100S_11_M1_46	2##2##2##6##(2017-03-30_Thu_32#1#1)(2018-04-09_Mon_5#1#1)
		
    	 */
		String[] QantryIDInfo = QantryID_TimeStamp.split(" "); 
    	String FirstQantryID = QantryIDInfo[0].substring(0,8); // "05F0001N"_00_M1_00
    	
    	//String FirstQantryID_Hour = QantryIDInfo[0].substring(9,11);
    	
    	String LastQantryID = QantryIDInfo[QantryIDInfo.length-1].substring(0,8);
    	
    	//if ((FirstQantryID.equals(StartQantryID))&&(LastQantryID.equals(EndQantryID)) ) {
    	if ((FirstQantryID.equals(StartQantryID))
    	  &&(LastQantryID.equals(EndQantryID))
    	  &&(LengthQantryID == QantryIDInfo.length)) {
        	
    		// 05F0001N_"00"_M1_00				
	    	String FirstQantryID_Hour = QantryIDInfo[0].substring(9,11); 
	    	// 05F0001N_00_M1_"00"
			String Min = QantryIDInfo[0].substring(15,17);
			
			int HourIndex = Integer.parseInt(FirstQantryID_Hour);
			int MinIndex = Integer.parseInt(Min);
			
			int OneDayMinIndex = HourIndex* 60 + MinIndex;
			
			//String QantryID_Pattern = FirstQantryID+" "+FirstQantryID_Hour;
			
			int PreviousMinIndex = OneDayMinIndex;
			
			String TimeIntervalSeq = "";
			int NextMinIndex = 0;
			int TotalTime = 0;
			int MaxDayMinIndex = 24*60;
			/*
			 * Compute the series of travel time intervals among GantryID
			 * e.g. "05F0001N_00_M1_00"
			 */
			// Compute "TotalTime" of travel time intervals with one MRP
	    	String IntevalTimes = "";
			for (int i=1;i<QantryIDInfo.length;i++) {
	    		
	    		String OneQantryID = QantryIDInfo[i].substring(0,8);
	    		String Hour = QantryIDInfo[i].substring(9,11);
	    		Min = QantryIDInfo[i].substring(15,17);
	    		
	    		HourIndex = Integer.parseInt(Hour);
	    		MinIndex = Integer.parseInt(Min);
	    		OneDayMinIndex = HourIndex* 60 + MinIndex;
	    		
	    		int OneTimeInterval = OneDayMinIndex - PreviousMinIndex;	
	    		if (OneTimeInterval < 0) {
	    			OneTimeInterval = MaxDayMinIndex - PreviousMinIndex + OneDayMinIndex;
	    		}		
	    		TotalTime = TotalTime + OneTimeInterval;
	    		
	    		IntevalTimes = IntevalTimes + "-"+ Integer.toString(OneTimeInterval);
	    	
	    		PreviousMinIndex = OneDayMinIndex;
	    	} // End of  for (int i=0;i<GateInfo.length;i++)
	    		    	
	    	
	    	//Text PatternKey = new Text(QantryIDInfo[0]+"#"+QantryIDInfo[QantryIDInfo.length-1]+"##"+TotalTime);
			Text PatternKey = new Text(FirstQantryID_Hour+"##"+Integer.toString(TotalTime)+"##"+IntevalTimes);
			//Text PatternKey = new Text(Integer.toString(TotalTime)+"##"+IntevalTimes);
	    
			//Text ValueKey = new Text(DF);
	    	Text ValueKey = new Text();
	    	
	    	  
			//PatternKey.set("");
			     
			//ValueKey.set(TF+"##"+CF+"##"+Length+"##"+IntevalTimes);
	    	//ValueKey.set(TF+"##"+CF+"##"+Length+"##"+ClassFrequencyDistribution);
	    	//ValueKey.set(TF+"##"+CF+"##"+Length);
	    	ValueKey.set(TF+"##"+CF+"##"+Length+"##"+ClassFrequencyDistribution);
	    				     
			context.write(PatternKey,ValueKey);
			    
    	} // End of if ((FirstQantryID.equals(StartQantryID))&&(LastQantryID.equals(EndQantryID)) )    	
	} //End of public void map
} // End of public class TDCS_MRP_Mapper
