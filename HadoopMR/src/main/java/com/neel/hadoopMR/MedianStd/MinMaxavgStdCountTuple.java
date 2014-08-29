package com.neel.hadoopMR.MedianStd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxavgStdCountTuple implements Writable {
	
	private int min;
	private int max;
	private int count;
	private int avg;
	private int std;
	public int getAvg() {
		return avg;
	}

	public void setAvg(int avg) {
		this.avg = avg;
	}

	public int getStd() {
		return std;
	}

	public void setStd(int std) {
		this.std = std;
	}


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
		avg = in.readInt();
		std = in.readInt();

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(getMax());
		out.writeInt(getMin());
		out.writeInt(getCount());
		out.writeInt(getAvg());
		out.writeInt(getStd());

	}
	public String toString() {
		return this.max+ "\t" + this.min + "\t" + this.count + "\t" + this.avg + "\t" + this.std;
		}

}
