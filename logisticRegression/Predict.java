package logisticRegression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

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
import utils.Tools;

public class Predict {
	public static class PredictMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		private double[] theta;
		private int num_attrs;
		private boolean compute_loss;// if compute_loss == true, <map> will get the label(if exists) and compute accuracy rate
		private int correct = 0;// correctly predict numbers of data
		private int count = 0;
		

		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			num_attrs = conf.getInt("num_attrs", 20);
			compute_loss = conf.getBoolean("compute_loss", true);
			theta = new double[num_attrs + 1];
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
			} catch (Exception e) {

			}
			
			// read converged theta from hdfs(need to be modified when trained over)
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/logistic_regression/iteration_2/part-r-00000"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line;
			line = bf.readLine();
			String[] attrs = line.split(",");
			for(int i=0;i<theta.length;i++) {
				theta[i] = Double.parseDouble(attrs[i]);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String attrs[] = value.toString().split(",");
			double[] X = new double[num_attrs + 1];
			X[0] = 1;
			for(int i=1;i<=num_attrs;i++) {
				X[i] = Double.parseDouble(attrs[i-1]);
			}
			
			// compute posterior of data, choose the max posterior
			double predict = Tools.sigmoid(Tools.dot(theta, X)) > 0.5? 1:0;
			
			if(compute_loss) {// compute cost and accuracy
				double Y = Double.parseDouble(attrs[num_attrs]);
				if(Y == predict) {
					correct++;
				}
				count++;
			}
			context.write(key, new Text(predict + ""));
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			if(compute_loss) {
				System.out.println("Accuracy rate:" + (double)correct/count);
			}
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
			for(Text line:values) {
				multipleOutputs.write(line, NullWritable.get(), "1170100513_4.txt");// write data in order
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Predict.class);
		job.setJobName("Predict");
		
		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/lab2/input/valid_data.txt"));
		Path output = new Path("/user/lab2/output/logistic_regression/predict");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
