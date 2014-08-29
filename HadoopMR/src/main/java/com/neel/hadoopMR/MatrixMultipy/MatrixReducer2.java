package com.neel.hadoopMR.MatrixMultipy;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer2 extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    
	private FloatWritable valueWritable =  new FloatWritable();
	public void reduce(Text key , Iterable<FloatWritable>value,Context context){
		
		float result = 0;
		
		for (FloatWritable val : value){
			String strvalue = val.toString();
			float intValue = Float.parseFloat(strvalue);
			result = result + intValue;
			
		}
		valueWritable.set(result);
		try {
			context.write(key,valueWritable);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
