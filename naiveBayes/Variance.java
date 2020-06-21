package naiveBayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//compute variance of every class's every dimension
public class Variance {
	public static class VarianceMapper extends Mapper<Object, Text, IntWritable, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int label = line.charAt(line.length()-1) - '0';// get label of data
			context.write(new IntWritable(label), value);// <label, data>
		}
	}
	
	public static class VariancesReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		
		private int num_attrs; // number of attributes(not including class label)
		private int num_labels;// number of labels
		private Configuration conf;
		private double[][] means;

		@Override
		protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
			num_attrs = conf.getInt("num_attrs", 20);
			num_labels = conf.getInt("num_labels", 2);
			means = new double[num_labels][num_attrs];
			
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (Exception e) {

			}
			// read means value from hdfs
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/naive_bayes/compute_means/means.txt"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line;
			while((line = bf.readLine()) != null) {
				String[] attrs = line.split(",");
				int label = Integer.parseInt(attrs[0]);
				int attr = Integer.parseInt(attrs[1]);
				double mean = Double.parseDouble(attrs[2]);
				means[label][attr] = mean;
			}
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			int label = key.get();
			int count = 0;
			double[] var = new double[num_attrs]; 
			for(Text line:values) {
				String[] attrs = line.toString().split(",");
				for(int i=0;i<num_attrs;i++) {
					double mean = means[label][i];
					var[i] += Math.pow(Double.parseDouble(attrs[i]) - mean, 2); // sum up (X(i) - Mu(i))^2
				}
				count++;
			}
			for(int i=0;i<num_attrs;i++) {
				var[i] /= count; // compute variance of every dimensioon
				multipleOutputs.write(new Text(label + "," + i + "," + var[i]), NullWritable.get(), "variances.txt");
			}
		}

		@Override
		protected void cleanup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
		
		
	}
}
