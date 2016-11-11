package com.bj58.shenji.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TrainDataByUser extends Configured implements Tool 
{
	public static class TrainDataByUserMapper extends Mapper<LongWritable,Text,Text,Text> 
	{
		private Text outKey = new Text();
		
		@Override
		public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException
		{
			String record = inValue.toString();
			outKey.set(record.substring(0, record.indexOf("\t")));
			context.write(outKey, inValue);
		}
	}
	
	public static class TrainDataByUserMapperReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException 
		{
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String cookieid = key.toString();
			
			for (Text value : values) {
				multipleOutputs.write(NullWritable.get(), value, cookieid);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			multipleOutputs.close();
		}
		
	}
	
	static class MultipleTextOutputFormatWithoutKey extends MultipleTextOutputFormat<Text, Text> {
		@Override
		public String generateFileNameForKeyValue(Text key, Text value, String name) {
			return key.toString();
		}
		
		@Override
		public Text generateActualKey(Text key, Text value) {
			return new Text();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		Configuration config = getConf();
		
		Job job = Job.getInstance(config);
		
		job.setJobName(getClass().getName());
		
		job.setJarByClass(TrainDataByUser.class);
		
		job.setMapperClass(TrainDataByUserMapper.class);
		job.setReducerClass(TrainDataByUserMapperReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(120);
		
		FileInputFormat.addInputPath(job, new Path("/home/team016/middata/test_train_data/"));  // 11075-19881166
		FileOutputFormat.setOutputPath(job, new Path("/home/team016/middata/traindatabyuser/"));// 11176-19881166
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception
	{
		int status = ToolRunner.run(new TrainDataByUser(), args);
		System.exit(status);
	}
}
