package logisticRegression;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LRReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
	private Configuration conf;
	private double[] old_theta;// old theta
	private double[] theta;// new theta
	private int num_attrs;// number of attributes(not including class label)

	@Override
	protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		num_attrs = conf.getInt("num_attrs", 20);
		theta = new double[num_attrs + 1];
		old_theta = new double[num_attrs + 1];
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int index = key.get();
		int count = 0;
		double means = 0.;
		for(Text value:values) {
			String[] two_theta = value.toString().split("_");
			old_theta[index] = Double.parseDouble(two_theta[0]);// get the old theta
			means += Double.parseDouble(two_theta[1]);// sum up the new theta
			count++;
		}
		theta[index] = means / count; // compute the means of all mapper's new theta
		System.out.println("theta[" + index + "]=" + theta[index]);
	}

	@Override
	protected void cleanup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		double cost = 0.;
		for(int i=0;i<theta.length;i++) {
			cost += Math.pow(theta[i] - old_theta[i], 2);
		}
		cost = Math.sqrt(cost);
		System.out.println("cost = " + cost);
		if(cost > conf.getDouble("delta", 0.3)) { // if error > delta, increment counter
			context.getCounter(LRCounter.CONVERGED).increment(1L);
		}
		context.write(new Text(StringUtils.join(theta, ',')), NullWritable.get());
	}
	
	
}
