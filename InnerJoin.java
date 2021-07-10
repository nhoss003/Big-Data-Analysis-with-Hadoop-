import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InnerJoin extends Configured implements Tool{
	public static String STATION="S=";
	public static String DATA="D=";
	
	public static int SID = 0;
	public static int DATE = 2;
	public static int TEMP = 3;
	public static int SAMPLES = 4;
	public static int STATE = 4;
	
	private static final String OUTPUT_PATH="intermediate_output";
	private static final String OUTPUTF_PATH="intermediate_outputf";
	
	public static class StationJoin extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			if(!arr[SID].equals("\"USAF\"") && !arr[STATE].equals("\"\"") && arr[3].equals("\"US\"")){
				//EMIT <SID, STATIONPREFIX + STATE>
				context.write(new Text(arr[SID].replaceAll("^\"|\"$", "")), new Text(STATION+arr[STATE].replaceAll("^\"|\"$", "")));
			}
		}
	}
	
	public static class DataJoin extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr = value.toString().split("\\s+");
			if(!arr[SID].equals("STN---")){
				//EMIT <SID, DATE + TEMP + SAMPLES>
				context.write(new Text(arr[SID]), new Text(DATA+arr[DATE]+ "," + arr[TEMP]));
			}
		}
	}
	
	public static class ReduceJoin extends Reducer<Text, Text, Text, Text>{
		public static int DATE=0;
		public static int TEMP=1;
		public static int SAMPLES=2;
		
		public void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException{
			HashMap<String,Double> numer = new HashMap<String,Double>();
			HashMap<String,Integer> denom = new HashMap<String,Integer>();
			String state="";
			for (Text val : value){//EXECUTE JOIN LOGIC
				String data = val.toString();
				if(data.startsWith(STATION)){//FIND STATE OF STATION
					state=data.replaceAll(STATION, "");
				}else if(data.startsWith(DATA)){//ADD/CREATE MONTH TEMPERATURE ENTRY
					data=data.replaceAll(DATA, "");
					String arr[] = data.split(",");
					String month = new DateFormatSymbols().getMonths()[Integer.valueOf(arr[DATE].substring(4,6)) - 1];
					if (!numer.containsKey(month)){
						numer.put(month, Double.valueOf(arr[TEMP]));
						denom.put(month, 1);
					}else{
						numer.put(month, numer.get(month)+Double.valueOf(arr[TEMP]));
						denom.put(month, denom.get(month)+1);
					}
				}
			}
			
			if(!state.equals("")){//EMIT <STATE, AVERAGE TEMP + MONTH>
				for(String month : numer.keySet()){
					context.write(new Text(state), new Text(month+"\t"+String.valueOf(numer.get(month)/denom.get(month))));
				}
			}
		}
	}
	
	public static class MaxMinTempMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr=value.toString().split("\\s+");
			context.write(new Text(arr[0]), new Text(arr[1]+" "+arr[2]));//EMIT <STATE,AVG TEMP + MONTH
		}
	}
	
	public static class MaxMinTempReducer extends Reducer<Text, Text, Text, Text>{
		public static int MONTH = 0;
		public static int TEMP = 1;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String,Double> numer = new HashMap<String,Double>();
			HashMap<String,Integer> denom = new HashMap<String,Integer>();
			
			for (Text val:values){//CALCULATE AVERAGE OF STATIONS AVERAGE TEMP
				String[] arrin=val.toString().split("\\s+");
				String month=arrin[MONTH];
				if (!numer.containsKey(month)){
					numer.put(month, Double.valueOf(arrin[TEMP]));
					denom.put(month, 1);
				}else{
					numer.put(month, numer.get(month)+Double.valueOf(arrin[TEMP]));
					denom.put(month, denom.get(month)+1);
				}
				
			}
			
			Double min=Double.MAX_VALUE;
			Double max=Double.MIN_VALUE;
			Double curr=0.0;
			String monthmin=null;
			String monthmax=null;
			for(String month : numer.keySet()){//FIND MAXIMUM TEMP MONTH AND MINIMUM TEMP MONTH PAIRS FOR STATE
				curr = numer.get(month)/denom.get(month);
				if( curr > max ){
					max = curr;
					monthmax = month;
				}
				
				if (curr < min){
					min = curr;
					monthmin = month;
				}
			}
			context.write(key, new Text(min+" "+monthmin+" "+max+" "+monthmax+" "+(max-min)));
		}
	}
	
	public static class FinalMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr=value.toString().split("\\s+");
			context.write(new DoubleWritable(Double.valueOf(arr[5])), new Text(arr[0]+" "+arr[1]+" "+arr[2]+" "+arr[3]+" "+arr[4]));
		}
	}
	
	public static class FinalReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val:values){
				context.write(key, val);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(),new InnerJoin(), args);
	}

	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		if(fs.exists(new Path(args[2]))){
			fs.delete(new Path(args[2]), true); 
		}
		
		Job job = Job.getInstance(getConf());
	    job.setJarByClass(InnerJoin.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(Integer.valueOf(args[3]));
	    
	    RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(args[0]), false);
	    while(it.hasNext()){
	    	 MultipleInputs.addInputPath(job, it.next().getPath(), TextInputFormat.class, DataJoin.class);
	    }
	    
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StationJoin.class);
	    job.setReducerClass(ReduceJoin.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
	    
	    job.waitForCompletion(true);
	    
	    ////////////////////////////////////////////////////////////////////
	    Job job2 = new Job(getConf(), "Job 2");
	    job2.setJarByClass(InnerJoin.class);

	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    
	    job2.setMapperClass(MaxMinTempMapper.class);
	    job2.setReducerClass(MaxMinTempReducer.class);
	    
	    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	   
	                               
	    FileOutputFormat.setOutputPath(job2, new Path(OUTPUTF_PATH));
	    job2.waitForCompletion(true);
	    fs.delete(new Path(OUTPUT_PATH), true); 
	    
	    Job job3 = new Job(getConf(), "Job 3");
	    job3.setJarByClass(InnerJoin.class);

	    job3.setInputFormatClass(TextInputFormat.class);
	    job3.setOutputFormatClass(TextOutputFormat.class);
	    
	    job3.setMapOutputKeyClass(DoubleWritable.class);
	    job3.setMapOutputValueClass(Text.class);
	    
	    job3.setOutputKeyClass(DoubleWritable.class);
	    job3.setOutputValueClass(Text.class);
	    
	    job3.setMapperClass(FinalMapper.class);
	    job3.setReducerClass(FinalReducer.class);
	    
	    FileInputFormat.addInputPath(job3, new Path(OUTPUTF_PATH));
	   
	                               
	    FileOutputFormat.setOutputPath(job3, new Path(args[2]));
	    job3.waitForCompletion(true);
	    fs.delete(new Path(OUTPUTF_PATH), true); 
	    
	    return 0;
	}
}
