package com.kartikiyer.hadoop.filemerge;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FileMergeReducer extends Reducer<LongWritable, Text, Text, NullWritable>
{
	@Override
	public void reduce(LongWritable key, Iterable<Text> vals, Context context) throws IOException, InterruptedException
	{
		for(Text value : vals)
		{
			context.write(value, NullWritable.get());
		}
	}
}
