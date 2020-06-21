package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

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

// predict data's label using converged controids
public class Predict {
	public static class PredictMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private ArrayList<Point> centroids;
		private int count = 0;
		private double cost = 0.;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			FileSystem fs = null;
			centroids = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
			} catch (Exception e) {

			}
			
			// read the converged controids(need to be modified when trained over)
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/kmeans/iteration_5/part-r-00000"));
			
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line;
			while((line = bf.readLine()) != null){
				centroids.add(Point.parse(line));
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Point point = Point.parse(value.toString());
			int index = 0;
			double minDistance = Double.MAX_VALUE;
			for(Point centroid:centroids) {// find the nearest centroid to the point
				double distence = point.getEuclDistance(centroid);
				if(distence < minDistance) {
					minDistance = distence;
					index = centroids.indexOf(centroid);
				}
			}
			cost += centroids.get(index).getEuclDistance(point);// compute average distance from points to controid
			count++;
			context.write(key, new Text(index + ""));// <line_bias, label>
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("cost = " + cost/count);
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
				multipleOutputs.write(text, NullWritable.get(), "1170100513_1.txt");// write data in order
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
		Path output = new Path("/user/lab2/output/kmeans/predict");
		
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
