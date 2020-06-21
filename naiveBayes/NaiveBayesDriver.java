package naiveBayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import utils.MyFileOutputFormat;

//run the naive bayes algorithm map-reduce and control the number of iteration 
public class NaiveBayesDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		conf.setInt("num_attrs", 20);
		conf.setInt("num_labels", 2);
		FileSystem fs = FileSystem.get(conf);
		
		//compute means
		Job compute_means = Job.getInstance(conf);
		compute_means.setJobName("compute means");
		compute_means.setMapperClass(Means.MeansMapper.class);
		compute_means.setReducerClass(Means.MeansReducer.class);
		compute_means.setJarByClass(NaiveBayesDriver.class);
		
		Path in = new Path("/user/lab2/input/train_data.txt");
		FileInputFormat.addInputPath(compute_means, in);
		Path out = new Path("/user/lab2/output/naive_bayes/compute_means");
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(compute_means, out);
		
		compute_means.setMapOutputKeyClass(IntWritable.class);
		compute_means.setMapOutputValueClass(Text.class);
		compute_means.setOutputKeyClass(Text.class);
		compute_means.setOutputValueClass(NullWritable.class);
		compute_means.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(compute_means, MyFileOutputFormat.class);
		
		compute_means.waitForCompletion(true);
		
		//compute variance
		Job compute_var = Job.getInstance(conf);
		compute_var.setJobName("compute variance");
		compute_var.setMapperClass(Variance.VarianceMapper.class);
		compute_var.setReducerClass(Variance.VariancesReducer.class);
		compute_var.setJarByClass(NaiveBayesDriver.class);
		
		in = new Path("/user/lab2/input/train_data.txt");
		FileInputFormat.addInputPath(compute_var, in);
		out = new Path("/user/lab2/output/naive_bayes/compute_var");
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(compute_var, out);
		
		compute_var.setMapOutputKeyClass(IntWritable.class);
		compute_var.setMapOutputValueClass(Text.class);
		compute_var.setOutputKeyClass(Text.class);
		compute_var.setOutputValueClass(NullWritable.class);
		compute_var.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(compute_var, MyFileOutputFormat.class);
		
		compute_var.waitForCompletion(true);
		
		//cross validation
		conf.setBoolean("compute_loss", true);// in cross validation, we need to know the cost and the accuracy
		Job valid = Job.getInstance(conf);
		valid.setJobName("cross validation");
		valid.setMapperClass(Predict.PredictMapper.class);
		valid.setReducerClass(Predict.PredictReducer.class);
		valid.setJarByClass(NaiveBayesDriver.class);
		
		in = new Path("/user/lab2/input/valid_data.txt");
		FileInputFormat.addInputPath(valid, in);
		out = new Path("/user/lab2/output/naive_bayes/cross_valid");
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(valid, out);
		
		valid.setMapOutputKeyClass(LongWritable.class);
		valid.setMapOutputValueClass(Text.class);
		valid.setOutputKeyClass(Text.class);
		valid.setOutputValueClass(NullWritable.class);
		valid.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(valid, MyFileOutputFormat.class);
		
		valid.waitForCompletion(true);
		
		//test
		conf.setBoolean("compute_loss", false);// in test, we shouldn't compute cost and accuracy
		Job test = Job.getInstance(conf);
		test.setJobName("predict");
		test.setMapperClass(Predict.PredictMapper.class);
		test.setReducerClass(Predict.PredictReducer.class);
		test.setJarByClass(NaiveBayesDriver.class);
		
		in = new Path("/user/lab2/input/test_data.txt");
		FileInputFormat.addInputPath(test, in);
		out = new Path("/user/lab2/output/naive_bayes/predict");
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(test, out);
		
		test.setMapOutputKeyClass(LongWritable.class);
		test.setMapOutputValueClass(Text.class);
		test.setOutputKeyClass(Text.class);
		test.setOutputValueClass(NullWritable.class);
		test.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(test, MyFileOutputFormat.class);
		
		test.waitForCompletion(true);
	}
}
