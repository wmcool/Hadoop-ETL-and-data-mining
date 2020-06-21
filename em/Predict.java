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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import utils.MyFileOutputFormat;
import utils.Point;
import utils.Tools;

//predict data's label using converged parameters
public class Predict {
	public static class PredictMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		private int k;
		private List<Point> mu;
		private List<Point> cov;
		private List<Double> pi;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
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
			
			//read parameters from hdfs(need to be modified when trained over)
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/em/iteration_8/part-r-00000"));
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
				k = mu.size();
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			Point x = Point.parse(value.toString());// transform a line of data to a point in n-dimensional space
			int index = 0;
			double max_posterior = -1;

			for (int i = 0; i < k; i++) {
				// compute pi_k * N(x_n|mu_k, sigma_k)
				double join_prob = pi.get(i)
						* Tools.multi_gaussian(x.getVector(), mu.get(i).getVector(), cov.get(i).getVector());
				// choose the max posterior probability
				if(join_prob > max_posterior) {
					index = i;
					max_posterior = join_prob;
				}
			}
			context.write(key, new Text(index + ""));
		}
	
	}
	
	public static class PredictReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		
		
		@Override
		protected void setup(Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
		}


		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for(Text text:values) {
				multipleOutputs.write(text, NullWritable.get(), "1170100513_2.txt");// write data in order
			}
		}


		@Override
		protected void cleanup(Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
		
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Predict.class);
		job.setJobName("Predict");
		
		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/lab2/input/cluster_data.txt"));
		Path output = new Path("/user/lab2/output/em/predict");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
