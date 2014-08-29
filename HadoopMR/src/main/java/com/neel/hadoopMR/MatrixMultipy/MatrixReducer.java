package com.neel.hadoopMR.MatrixMultipy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends Reducer<Text, Text, Text, FloatWritable> {

	
	public void reduce(Text key , Iterable<Text>value,Context context){
		
		List<Map<Integer,Float>> list1 = new ArrayList<Map<Integer,Float>>();
		List<Map<Integer,Float>> list2 = new ArrayList<Map<Integer,Float>>();
		
		for (Text val : value){
			 
			String line[] = val.toString().split(",");
			System.out.println(line[0] + " in reducer" + line[1] + " " +line[2]);
			
			if(line[0].equals("file1")){
				Map<Integer,Float> mapentry = new HashMap<Integer, Float>();
				mapentry.put(Integer.parseInt(line[1]),Float.parseFloat(line[2]));
				list1.add(mapentry);
			}
			else if(line[0].equals("file2"))
			{
				Map<Integer,Float> mapentry = new HashMap<Integer, Float>();
				mapentry.put(Integer.parseInt(line[1]),Float.parseFloat(line[2]));
				list2.add(mapentry);
			}
			
			int i = 0;
            float a_ij = 0;
            String k = null;
            float b_jk = 0;
            FloatWritable outputValue = new FloatWritable();
            Text key2 = new Text();
            for (Map<Integer, Float> a : list1) {
            	System.out.println(a);
            	  for(Map.Entry<Integer, Float> entry : a.entrySet()){
            		  i = entry.getKey();
                      a_ij = entry.getValue();
                      System.out.println("print a_ij" + a_ij);
            	  }
                
                for (Map<Integer, Float> b : list2) {
                	System.out.println(b);
                	for(Map.Entry<Integer, Float> entry : b.entrySet()){
                		 k = Integer.toString(entry.getKey());
                         b_jk = entry.getValue();
                         System.out.println("print b_ij" + b_jk);
              	  }
                	key2.set(i + "," + k);
                	  System.out.println("print a*b_ij" + a_ij*b_jk);
                    outputValue.set(a_ij*b_jk);
                    try {
						context.write(key2, outputValue);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                }
            }
		}
	}
}
