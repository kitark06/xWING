package com.kartikiyer.hadoop.filemerge;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FileMerge extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");

		ToolRunner.run(new FileMerge(), args);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = Job.getInstance();
		job.setJobName("FileMerger_Combiner");

		job.getConfiguration().set("mapreduce.app-submission.cross-platform", "true");
		job.getConfiguration().set("mapreduce.map.memory.mb", "1024");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "1024");

		job.setJarByClass(FileMerge.class);
		job.setMapperClass(FileMergeMapper.class);
		job.setReducerClass(FileMergeReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(Converger.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);


		Path in = new Path("/user/cloudera/Small_Files");
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path("/user/cloudera/mergedop");

		if (fs.exists(out))
			fs.delete(out, true);

		FileOutputFormat.setOutputPath(job, out);

		System.out.println("Starting job");

		job.submit();
		return 1;
//		return job.waitForCompletion(false) ? 1 : 0;
	}
}
