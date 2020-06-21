package em;

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

//run the em algorithm map-reduce and control the number of iteration 
public class EMDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int iter = 1;
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		conf.setDouble("delta", 0.5);// if error is less than delta, stop the algorithm
		conf.set("attrs_path", "/user/lab2/input/init_mu_cov_pi.txt");// initial mu, sigma and pi
		long counter;
		FileSystem fs = FileSystem.get(conf);
		do {
			Job job = Job.getInstance(conf);
			job.setJobName("EM Clustering " + iter);
			
			job.setMapperClass(EMMapper.class);
			job.setReducerClass(EMReducer.class);
			job.setJarByClass(EMDriver.class);
			
			Path in = new Path("/user/lab2/input/cluster_data.txt");
			Path out = new Path("/user/lab2/output/em/iteration_" + iter + "/");
			FileInputFormat.addInputPath(job, in);
			if(fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Stats.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.waitForCompletion(true);
			
			counter = job.getCounters().findCounter(EMCounter.CONVERGED).getValue();
			System.out.println("counter = " + counter);
			
			// set the parameter's path for next iteration
			conf.set("attrs_path", "/user/lab2/output/em/iteration_" + iter + "/part-r-00000");
			iter++;
		} while(counter > 0 && iter<15);
	}
}
