package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Point;

// run the Kmeans map-reduce and control the number of iteration 
public class KMeansDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int iter = 1;
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		conf.setDouble("delta", 0.1);// if error is less than delta, stop the algorithm
		conf.set("centroid_path", "/user/lab2/input/init_8_centroids.txt");// initial centroids 
		long counter;
		FileSystem fs = FileSystem.get(conf);
		do {
			Job job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iter);
			
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansDriver.class);
			
			Path in = new Path("/user/lab2/input/cluster_data.txt");
			Path out = new Path("/user/lab2/output/kmeans/iteration_" + iter + "/");
			FileInputFormat.addInputPath(job, in);
			if(fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			
			job.setMapOutputKeyClass(Point.class);
			job.setMapOutputValueClass(Point.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.waitForCompletion(true);
			
			counter = job.getCounters().findCounter(KmeansCounter.CONVERGED).getValue();
			System.out.println("counter = " + counter);
			
			// set the centroids path for next iteration
			conf.set("centroid_path", "/user/lab2/output/kmeans/iteration_" + iter + "/part-r-00000");
			iter++;
			conf.setInt("iteration", iter);
		}while(counter > 0 && iter<15);
	}
}
