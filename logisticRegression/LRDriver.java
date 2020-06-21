package logisticRegression;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//run the logistic regression map-reduce and control the number of iteration 
public class LRDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int iter = 1;
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		conf.setInt("iteration", iter);
		conf.setDouble("delta", 0.1);// if error is less than delta, stop the algorithm
		conf.setDouble("alpha", 0.3);// learning rate
		conf.setInt("num_attrs", 20);
		conf.set("theta_path", "/user/lab2/input/init_theta.txt");
		long counter;
		FileSystem fs = FileSystem.get(conf);
		do {
			Job job = Job.getInstance(conf);
			job.setJobName("Logistic Regression " + iter);
			
			job.setMapperClass(LRMapper.class);
			job.setReducerClass(LRReducer.class);
			job.setJarByClass(LRDriver.class);
			
			Path in = new Path("/user/lab2/input/train_data.txt");
			Path out = new Path("/user/lab2/output/logistic_regression/iteration_" + iter + "/");
			FileInputFormat.addInputPath(job, in);
			if(fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(1);
			job.waitForCompletion(true);
			
			counter = job.getCounters().findCounter(LRCounter.CONVERGED).getValue();
			System.out.println("counter = " + counter);
			
			// set the parameter's path for next iteration
			conf.set("theta_path", "/user/lab2/output/logistic_regression/iteration_" + iter + "/part-r-00000");
			iter++;
			conf.setInt("iteration", iter);
		}while(counter > 0);
	}
}
