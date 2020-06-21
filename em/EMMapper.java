package em;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Point;
import utils.Tools;

public class EMMapper extends Mapper<Object, Text, IntWritable, Stats> {

	private int k;
	private List<Point> mu;
	private List<Point> cov;
	private List<Double> pi;

	@Override
	protected void setup(Mapper<Object, Text, IntWritable, Stats>.Context context)
			throws IOException, InterruptedException {
		FileSystem fs = null;
		Configuration conf = context.getConfiguration();
		mu = new ArrayList<>();
		cov = new ArrayList<>();
		pi = new ArrayList<>();
		try {
			fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		} catch (Exception e) {

		}
		FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/" + conf.get("attrs_path")));// read mu, sigma pi from hdfs
		BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
		String line;
		while (true) {
			if ((line = bf.readLine()) == null) {
				break;
			}
			// read 3 lines once, mu, sigma and pi
			mu.add(Point.parse(line));
			line = bf.readLine();
			cov.add(Point.parse(line));
			line = bf.readLine();
			pi.add(Double.parseDouble(line.trim()));
		}
		k = mu.size();
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Stats>.Context context)
			throws IOException, InterruptedException {
		
		Point x = Point.parse(value.toString());// transform a line of data to a point in n-dimensional space
		double[] posterior = new double[k];// posterior probability that the point belongs to every centroid
		double px = 0;

		for (int i = 0; i < k; i++) {
			// compute pi_k * N(x_n|mu_k, sigma_k)
			double join_prob = pi.get(i)
					* Tools.multi_gaussian(x.getVector(), mu.get(i).getVector(), cov.get(i).getVector());
			posterior[i] = join_prob;
			px += join_prob;
		}
		if(px == 0) {// join_prob may be very small (double number can't represent)
			return;
		}
		for (int i = 0; i < k; i++) {
			posterior[i] /= px;// compute posterior probability
			context.write(new IntWritable(i), new Stats(posterior[i], x.getVector(), mu.get(i).getVector()));
		}
	}

}
