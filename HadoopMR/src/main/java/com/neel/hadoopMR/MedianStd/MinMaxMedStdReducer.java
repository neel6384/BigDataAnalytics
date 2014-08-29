package com.neel.hadoopMR.MedianStd;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxMedStdReducer extends Reducer<LongWritable, MinMaxavgStdCountTuple, LongWritable, MinMaxavgStdCountTuple> {
	
	private MinMaxavgStdCountTuple result = new MinMaxavgStdCountTuple();
	private ArrayList<Float> temperature = new ArrayList<Float>();
	
	public void reduce(LongWritable key, Iterable<MinMaxavgStdCountTuple> values,
	Context context) throws IOException, InterruptedException {
	
		result.setMin(0);
		result.setMax(0);
		result.setCount(0);
		result.setAvg(0);
		result.setStd(0);
		temperature.clear();
		
		int sum = 0;
		
		/*for (MinMaxavgStdCountTuple val : values){
			
			if(result.getMin()>val.getMin()){
				result.setMin(val.getMin());
			}
			if(result.getMax() < val.getMax()){
				result.setMax(val.getMax());
			}
			sum = sum + val.getCount();
			temperature.add((float) val.);
		}*/
		result.setCount(sum);
		context.write(key, result);
	
	}
	
}
