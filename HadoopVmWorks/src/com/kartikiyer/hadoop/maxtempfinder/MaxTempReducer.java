package com.kartikiyer.hadoop.maxtempfinder;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, Text, Text, Text>
{
	@Override
	protected void reduce(Text year, Iterable<Text> tempList, Context context) throws IOException, InterruptedException
	{
		Float maxValue = Float.MIN_VALUE;
		Text maxTemp = new Text();
		
//		Iterator temps = temps.iterator();
		
		for(Text temp : tempList)
		{
			Float currentTemp = Float.parseFloat(temp.toString());
			if(currentTemp > maxValue && currentTemp < 100.00)
				maxValue = currentTemp;
		}
		
		maxTemp.set(String.valueOf(maxValue));
		context.write(year, maxTemp);
				
	}
}
