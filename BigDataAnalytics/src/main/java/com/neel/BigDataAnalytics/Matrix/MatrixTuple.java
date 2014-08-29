package com.neel.BigDataAnalytics.Matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatrixTuple implements Writable {
    
	private String matrixName;
	private int colrowid ;
	private int value;
	
	public String getMatrixName() {
		return matrixName;
	}

	public void setMatrixName(String matrixName) {
		this.matrixName = matrixName;
	}

	public int getColrowid() {
		return colrowid;
	}

	public void setColrowid(int colrowid) {
		this.colrowid = colrowid;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		colrowid = in.readInt();
		value = in.readInt();
		matrixName = in.readLine();

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
       out.writeInt(getColrowid());
       out.writeInt(getValue());
       out.writeChars(getMatrixName());
	}
   
	public String toString(){
		return "Matrix name :" + this.matrixName + "Row/Column" + this.colrowid
				+ "Value" + this.value;
		
	}
}
