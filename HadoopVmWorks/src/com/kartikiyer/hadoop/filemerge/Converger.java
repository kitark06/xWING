package com.kartikiyer.hadoop.filemerge;

import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class Converger extends CombineTextInputFormat
{
	public Converger()
	{
		// setting block size to 128mb which is the Cloudera default HDFS size
		this.setMaxSplitSize(134217728L);
	}
}
