package logisticRegression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Tools;

public class LRMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private double[] old_theta;// old theta
	private double[] theta;// new theta
	private int num_attrs;// number of attributes(not including class label)
	private double alpha;// learning rate
	private Configuration conf;
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		num_attrs = conf.getInt("num_attrs", 20);
		alpha = conf.getDouble("alpha", 1);
		old_theta = new double[num_attrs + 1];
		theta = new double[num_attrs + 1]; // theta_0 is 21th dimension
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		} catch (Exception e) {

		}
		
		// read theta from hdfs
		FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/" + conf.get("theta_path")));
		BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
		String line;
		line = bf.readLine();
		String[] attrs = line.split(",");
		for(int i=0;i<theta.length;i++) {
			theta[i] = Double.parseDouble(attrs[i]);
			old_theta[i] = theta[i];
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String attrs[] = value.toString().split(",");
		double[] X = new double[num_attrs + 1];
		
		X[0] = 1;
		for(int i=1;i<=num_attrs;i++) {
			X[i] = Double.parseDouble(attrs[i-1]);
		}
		double Y = Double.parseDouble(attrs[num_attrs]);
		double error = Tools.sigmoid(Tools.dot(theta, X)) - Y;//compute h_theta(x) - y
		
		for(int j=0;j<theta.length;j++) {
			double theta_deriv = error * X[j];// compute derivative
			theta[j] -= alpha * theta_deriv;// gradient descent
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for(int i=0;i<theta.length;i++) {
			context.write(new IntWritable(i), new Text(old_theta[i] + "_" + theta[i]));// <i, old_theta[i] and new_theta[i]>
		}
	}
	
}
