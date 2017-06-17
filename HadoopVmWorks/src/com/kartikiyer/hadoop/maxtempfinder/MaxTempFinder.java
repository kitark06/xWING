package com.kartikiyer.hadoop.maxtempfinder;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.kartikiyer.hadoop.wordcount.WordCountClient;

public class MaxTempFinder extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");
		ToolRunner.run(new MaxTempFinder(), args);
	}
	

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = Job.getInstance();
		job.setJobName("MaxTemp");

		job.getConfiguration().set("mapreduce.app-submission.cross-platform", "true");
		job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");

		job.setJarByClass(getClass());
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path in = new Path("/user/kartik.iyer/weatherInput/files");
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path("/user/kartik.iyer/maxweatheroutput");

		if (fs.exists(out))
			fs.delete(out, true);

		FileOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
}
