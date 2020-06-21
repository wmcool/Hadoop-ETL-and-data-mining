package em;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

// used to store parameter in shuffle
public class Stats implements Serializable, Writable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private double posterior;
	private ArrayPrimitiveWritable x = null;
	private ArrayPrimitiveWritable mu = null;
	
	public Stats() {
		this.x = new ArrayPrimitiveWritable();
		this.mu = new ArrayPrimitiveWritable();
	}
	public Stats(double posterior, double[] x, double[] mu) {
		this.posterior = posterior;
		this.x = new ArrayPrimitiveWritable();
		this.mu = new ArrayPrimitiveWritable();
		this.x.set(x);
		this.mu.set(mu);
	}

	public double getPosterior() {
		return posterior;
	}

	public void setPosterior(double posterior) {
		this.posterior = posterior;
	}
	
	public double[] getX() {
		return (double[])x.get();
	}

	public double[] getMu() {
		return (double[])mu.get();
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		posterior = arg0.readDouble();
		x.readFields(arg0);
		mu.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(posterior);
		x.write(arg0);
		mu.write(arg0);
	}
	
	
}
