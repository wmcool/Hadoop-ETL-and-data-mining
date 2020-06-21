package em;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EMReducer extends Reducer<IntWritable, Stats, Text, NullWritable> {
	private int num_attrs;
	private double delta;

	@Override
	protected void setup(Reducer<IntWritable, Stats, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		num_attrs = conf.getInt("num_attrs", 20);// number of attributes
		delta = conf.getDouble("delta", 1);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Stats> values,
			Reducer<IntWritable, Stats, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		double Nk = 0;
		int N = 0;
		double posterior;
		double[] mu_new = new double[num_attrs];
		double[] cov_new = new double[num_attrs];
		double pi;
		double[] mu = null;
		double[] x = null;
		for (Stats stat : values) {
			posterior = stat.getPosterior();// get gamma(z_nk)
			x = stat.getX();// get x_n
			mu = stat.getMu();// get old mu

			for (int i = 0; i < num_attrs; i++) {
				mu_new[i] += posterior * x[i];
				double error = x[i] - mu[i];
				cov_new[i] += posterior * error * error;
			}
			Nk += posterior;// compute N_k
			N++;
		}

		for (int i = 0; i < num_attrs; i++) {
			mu_new[i] /= Nk;// compute new mu
			System.out.print(mu_new[i]);
			cov_new[i] /= Nk;// compute new sigma
		}
		pi = Nk / N;// compute new pi
		System.out.println("   " + getDistence(mu, mu_new));
		
		// if error > delta, increment counter
		if (getDistence(mu, mu_new) > delta) {
			context.getCounter(EMCounter.CONVERGED).increment(1L);
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(StringUtils.join(ArrayUtils.toObject(mu_new), ",") + "\n");
		sb.append(StringUtils.join(ArrayUtils.toObject(cov_new), ",") + "\n");
		sb.append(pi);// write back mu, sigma, pi

		context.write(new Text(sb.toString()), NullWritable.get());
	}

	/*
	 * get the euclidean distance for two vector
	 */
	public static double getDistence(double[] x, double[] y) {
		if (x.length != y.length)
			return -1;
		int n = x.length;
		double distence = 0;
		for (int i = 0; i < n; i++) {
			distence += Math.pow(x[i] - y[i], 2);
		}
		return Math.sqrt(distence);
	}
}
