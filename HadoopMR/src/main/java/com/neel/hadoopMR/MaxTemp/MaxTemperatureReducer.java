package com.neel.hadoopMR.MaxTemp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, Text> {
  
	private static ArrayList<Integer> temValues = new ArrayList<Integer>();
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;
    int sum = 0;
    int count = 0;
     
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
      minValue = Math.min(minValue, value.get());
      sum = sum + value.get();
      count = count + 1;
      temValues.add(value.get());
    }
    
     float mean = sum/count;
     double sumOfSquare = 0.0;
     
     for(double d : temValues){
    	 sumOfSquare = sumOfSquare + (d-mean)*(d-mean);
     }
     
    double sd = Math.sqrt(sumOfSquare/(count-1));
    
    Text result = new Text();
    StringBuilder sb = new StringBuilder();
    sb.append(maxValue).append("\t").append(minValue).append("\t").append(mean).append("\t").append(sd);
    
    result.set(sb.toString());
    
    context.write(key, result);
  }
}