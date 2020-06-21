package utils;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

// a point in n-dimensional space
public class Point implements Writable, WritableComparable<Point>{
    private ArrayPrimitiveWritable vector = null;// every dimension's value

    public Point(){
        vector = new ArrayPrimitiveWritable();
    }
    public Point(double[] vector) {
    	this.vector = new ArrayPrimitiveWritable();
        setVector(vector);
    }
    public Point(Point point){
        double[] vector = point.getVector();
        setVector(Arrays.copyOf(vector, vector.length));
    }

    public double[] getVector() {
        return (double[]) vector.get();
    }

    public void setVector(double[] vector) {
        this.vector.set(vector);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        vector.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vector.readFields(dataInput);
    }

    @Override
    public String toString() {
        double[] thisVector = this.getVector();
        StringBuilder sb = new StringBuilder();
        for (int i = 0, j = thisVector.length; i < j; i++) {
            sb.append(thisVector[i]);
            if (i < thisVector.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /*
     * parse point in string to point in vector
     */
    public static Point parse(String values) {
        String[] attrs = values.split(",");
        double[] tmp = new double[attrs.length];
        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = Double.valueOf(attrs[i]);
        }
        return new Point(tmp);
    }
    
    /*
     * get Euclidean distance to another point
     */
    public double getEuclDistance(Point p){
        double[] pVector = p.getVector();

        if (getVector().length != pVector.length) {
        	return -1;
        }

        double sum = 0;
        for (int i = 0; i < getVector().length; i++){
            sum += Math.pow(getVector()[i] - pVector[i], 2);
        }

        return Math.sqrt(sum);
    }

    /*
     * add another point's every dimension's value to this point
     */
    public void add(Point point){
    	if(getVector() == null) {
    		setVector(point.getVector());
    		return;
    	}
        double[] thisVector = this.getVector();
        double[] pointVector = point.getVector();
        for (int i =0; i<thisVector.length; i++){
            thisVector[i] += pointVector[i];
        }
    }

    /*
     * divide every dimension's value by n
     */
    public void divide(int n) {
    	double [] thisVector = this.getVector();
    	for(int i=0;i<thisVector.length;i++) {
    		thisVector[i] /= n;
    	}
    }

	@Override
	public int compareTo(Point o) {
		if(this.getVector()[0] > o.getVector()[0]) {
			return 1;
		}else if(this.getVector()[0] == o.getVector()[0]) {
			return 0;
		}else {
			return -1;
		}
	}

}
