package preprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * predict missing value on rating and user_income using mean value
 */
public class Predict {

	public static class PredictMapper extends Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private double predict_rating = 0;
		private double predict_income = 0;

		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (Exception e) {

			}
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/hduser_/output/Normal/Mean"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line = bf.readLine();
			String[] maxmin = line.split(" ");
			//get mean value of rating and income
			predict_rating = Double.parseDouble(maxmin[0]);
			predict_income = Double.parseDouble(maxmin[1]);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line.length() <= 1)
				return;// filter \n
			String[] attr = line.split("\\|");

			//predict rating
			String s_rating = attr[6];
			if (s_rating.equals("?")) {
				attr[6] = predict_rating + "";
			}

			//predict income
			String s_income = attr[11];
			if (s_income.equals("?")) {
				attr[11] = predict_income + "";
			}
			line = StringUtils.join(attr, '|');
			multipleOutputs.write(new Text(line), NullWritable.get(), "D_Done");
		}

		@Override
		protected void cleanup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
		}

	}

	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.err.println("Usage: Sample && filter <input path> <output path>");
//			System.exit(-1);
//		}
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-3.2.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Predict.class);
		job.setJobName("Predict");

		job.setMapperClass(PredictMapper.class);

		FileInputFormat.addInputPath(job, new Path("/user/hduser_/output/Normal/D_Filter"));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("/user/hduser_/output/Predict"))) {
			fs.delete(new Path("/user/hduser_/output/Predict"), true);
		}
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser_/output/Predict"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
