package com.neel.hadoopMR.MinMaxCountTuple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxReducer extends Reducer<LongWritable, MinMaxCountTuple, LongWritable, MinMaxCountTuple> {
	
	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	public void reduce(LongWritable key, Iterable<MinMaxCountTuple> values,
	Context context) throws IOException, InterruptedException {
	
		result.setMin(0);
		result.setMax(0);
		result.setCount(0);
		int sum = 0;
		
		for (MinMaxCountTuple val : values){
			
			if(result.getMin()>val.getMin()){
				result.setMin(val.getMin());
			}
			if(result.getMax() < val.getMax()){
				result.setMax(val.getMax());
			}
			sum = sum + val.getCount();
		}
		result.setCount(sum);
		context.write(key, result);
	
	}
	
}
