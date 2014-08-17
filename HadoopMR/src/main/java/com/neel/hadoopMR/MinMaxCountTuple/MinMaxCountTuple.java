package com.neel.hadoopMR.MinMaxCountTuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {
	
	private int min;
	private int max;
	private int count;
	

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
	
		min = in.readInt();
		max = in.readInt();
		count = in.readInt();

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(getMax());
		out.writeInt(getMin());
		out.writeInt(getCount());

	}
	public String toString() {
		return this.max+ "\t" + this.min + "\t" + this.count;
		}

}
