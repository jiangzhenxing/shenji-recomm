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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
			// r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime, position, enterprise
			String[] values = inValue.toString().split("\001", 10);
			outKey.set(values[0] + ":" + values[4]);
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
			String cookieid = key.toString().split(":")[0];
			
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
	
	/**
	 * 分区函数，按Cookieid分区
	 * @author jiangzhenxing
	 * @version 2015年8月5日
	 */
	public static class CookieidPartitioner extends Partitioner<Text, Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return Math.abs(key.toString().split(":")[0].hashCode()) % numPartitions;
		}
	}
	
	/**
	 * 分组函数，按Cookieid分组
	 * @author jiangzhenxing
	 * @version 2015年8月5日
	 */
	public static class CookieidGroupComparator extends WritableComparator
	{
		private static final Text.Comparator COMPARATOR = new Text.Comparator();
		
		public CookieidGroupComparator() {
			super(Text.class);
		}
		
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) 
		{
			if (o1 instanceof Text && o2 instanceof Text) {
				String k1 = o1.toString();
				String k2 = o2.toString();
				
				return ((Text) o1).compareTo(((Text) o2));
			}
			return super.compare(o1, o2);
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
		
		Job job = Job.getInstance(config);
		
		job.setJobName(getClass().getName());
		
		job.setJarByClass(TrainDataByUser.class);
		
		job.setMapperClass(TrainDataByUserMapper.class);
		job.setReducerClass(TrainDataByUserMapperReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(CookieidPartitioner.class);
		job.setNumReduceTasks(200);
		
		for (int i = 1; i < 16; i++)
			FileInputFormat.addInputPath(job, new Path("/home/team016/middata/stage2/traindata/dt=" + i));  // 23497 11075-19881166
		
		FileOutputFormat.setOutputPath(job, new Path("/home/team016/middata/stage2/traindatabyuser/"));// 23699  11176-19881166
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception
	{
		int status = ToolRunner.run(new TrainDataByUser(), args);
		System.exit(status);
	}
}
