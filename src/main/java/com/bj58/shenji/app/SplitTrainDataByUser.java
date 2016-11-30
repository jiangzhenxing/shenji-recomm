package com.bj58.shenji.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SplitTrainDataByUser extends Configured implements Tool 
{
	public static class TrainDataByUserMapper extends Mapper<LongWritable,Text,Text,Text> 
	{
		private Text outKey = new Text();
		
		@Override
		public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException
		{
			// r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime, position, enterprise
			String[] values = inValue.toString().split("\001", 10);
			outKey.set(values[0]);
			context.write(outKey, inValue);
		}
	}
	
	public static class TrainDataByUserMapperReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		private Text out = new Text();
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException 
		{
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String cookieid = key.toString();
			List<String> records = new ArrayList<String>();
			
			// r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime
			for (Text value : values) {
				multipleOutputs.write(NullWritable.get(), out, cookieid);
			}
			
			Collections.sort(records, new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					return (int) (Long.parseLong(s1.split("\001", 10)[4]) - Long.parseLong(s2.split("\001", 10)[4]));
				}
			});
			
			int train = (int) (records.size() * 0.8);
			
			for (int i = 0; i < train; i++) {
				out.set(records.get(i));
				multipleOutputs.write(NullWritable.get(), out, "train80/" + cookieid);
			}
			
			for (int i = train; i < records.size(); i++) {
				out.set(records.get(i));
				multipleOutputs.write(NullWritable.get(), out, "train20/" + cookieid);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			multipleOutputs.close();
		}
		
	}
	
	/*
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
	*/
	@Override
	public int run(String[] args) throws Exception
	{
		Configuration config = getConf();
		config.set("mapreduce.reduce.java.opts", "-Xmx8000m");
		config.set("mapreduce.reduce.memory.mb", "8000m");
		
		Job job = Job.getInstance(config);
		
		job.setJobName(getClass().getName());
		
		job.setJarByClass(SplitTrainDataByUser.class);
		
		job.setMapperClass(TrainDataByUserMapper.class);
		job.setReducerClass(TrainDataByUserMapperReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(50);
		
		FileInputFormat.addInputPath(job, new Path("/home/team016/middata/stage2/traindatabyuser/"));  // 23497 11075-19881166
		
		FileOutputFormat.setOutputPath(job, new Path("/home/team016/middata/stage2/traindatabyuser_split/"));// 23699  11176-19881166
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception
	{
		int status = ToolRunner.run(new SplitTrainDataByUser(), args);
		System.exit(status);
	}
}
