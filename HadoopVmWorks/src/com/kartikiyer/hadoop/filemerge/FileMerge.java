package com.kartikiyer.hadoop.filemerge;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class FileMerge
{
	public static void main(String[] args) throws Exception
	{
		System.setProperty("hadoop.home.dir", "C:\\winutil\\");
		System.setProperty("HADOOP_USER_NAME", "cloudera");

		Job job = Job.getInstance();
		job.setJobName("FileMerger");

		job.getConfiguration().set("mapreduce.app-submission.cross-platform", "true");
//		job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
		

		job.setJarByClass(FileMerge.class);
		job.setMapperClass(FileMergeMapper.class);
		job.setReducerClass(FileMergeReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);


		Path in = new Path("/user/cloudera/weatherInput/files1");
//		Path in = new Path("/user/kartik.iyer/weatherInput/files");
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path("/user/cloudera/weatherOutput");

		if (fs.exists(out))
			fs.delete(out, true);

		FileOutputFormat.setOutputPath(job, out);

		System.out.println("Starting job");
		job.waitForCompletion(true);

	}
}
