package com.kartikiyer.hadoop.maxtempfinder;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTempMapper extends Mapper<LongWritable, Text, Text, Text>
{
	Text	year	= new Text();
	Text	temp	= new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();

		if (line.length() > 58)
		{
			year.set(line.substring(13, 21));
			temp.set(line.substring(51, 57));
			context.write(year, temp);
		}
	}
}
