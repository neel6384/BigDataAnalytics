package com.neel.hadoop.HadoopMR.MaxTemp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Check {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
 
	
		BufferedReader reader =  new BufferedReader( new FileReader(new File("/home/neel/1901")));
	    
		String line = null;
		
		while((line=reader.readLine())!=null){
			String year  = line.substring(0,4);
			String tm  = line.substring(13,19);
			System.out.println(year + ">>"+ tm);
		}
	}

}
