package com.neel.hadoopMR.MinMaxCountTuple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<LongWritable, Text, LongWritable, MinMaxCountTuple> {

	private LongWritable outYear= new LongWritable();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	public void map(LongWritable key,Text value, Context context){
		
		   
	    String line = value.toString();
		String year  = line.substring(0,4);
		
	    int airTemperature  = Integer.parseInt(line.substring(13,19).trim());
		// Set the minimum and maximum date values to the air temperature
	    if (airTemperature!= 9999 && airTemperature!= -9999){
	    	outTuple.setMin(airTemperature);
			outTuple.setMax(airTemperature);	
	    }
	    outYear.set(Integer.parseInt(year));
		outTuple.setCount(1);
		
		try {
			context.write(outYear, outTuple);
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
}
