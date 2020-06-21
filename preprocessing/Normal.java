package preprocessing;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * 1.normalize rating with max and min
 * 2.compute mean of rating and user_income
 */

public class Normal {

	public static class NormalMapper extends Mapper<Object, Text, Mean, Text> {
		
		private double rating_max = 0;
		private double rating_min = 0; 

		@Override
		protected void setup(Mapper<Object, Text, Mean, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (Exception e) {

			}
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/hduser_/output/Analyse_attr/Max_Min"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line = bf.readLine();
			String[] maxmin = line.split(" ");
			
			//get max and min from hdfs
			rating_max = Double.parseDouble(maxmin[0]);
			rating_min = Double.parseDouble(maxmin[1]);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Mean mean = new Mean();
			String line = value.toString();
			if(line.length() <= 1) return;//filter \n
			String[] attr = line.split("\\|");
			
			//normalize
			String s_rating = attr[6];
			if(s_rating.equals("?")) {
				context.write(new Mean(), value);
				return;
			}
			double rating = Double.parseDouble(attr[6]);
			rating = (rating - rating_min) / (rating_max - rating_min);
			
			String s_income = attr[11];
			if(s_income.equals("?")) {
				context.write(new Mean(), value);
				return;
			}
			double income = Double.parseDouble(attr[11]);
			
			mean.addTuple(rating, income);
			
			attr[6] = rating + "";
			line = StringUtils.join(attr, '|');
			context.write(mean, new Text(line));
		}
		@Override
		protected void cleanup(Mapper<Object, Text, Mean, Text>.Context context)
				throws IOException, InterruptedException {
		}
		
		
	}

	public static class NormalReducer extends Reducer<Mean, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private Mean result = new Mean();

		@Override
		protected void setup(Reducer<Mean, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
		}

		@Override
		protected void reduce(Mean key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for(Text line:values) {
				//output the normalized line
				multipleOutputs.write(line, NullWritable.get(), "D_Filter");
				
				//add the line's value to result
				result.add(key);
			}
		}

		@Override
		protected void cleanup(Reducer<Mean, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			//compute mean value of rating and income
			double mean_rating = result.rating_mean();
			double mean_income = result.income_mean();
			multipleOutputs.write(new Text(mean_rating + " " + mean_income), NullWritable.get(), "Mean");
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
		job.setJarByClass(Normal.class);
		job.setJobName("Normalize");

		job.setMapperClass(NormalMapper.class);
		job.setReducerClass(NormalReducer.class);

		FileInputFormat.addInputPath(job, new Path("/user/hduser_/output/Sample_Filter/D_Filter"));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("/user/hduser_/output/Normal"))) {
			fs.delete(new Path("/user/hduser_/output/Normal"), true);
		}
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser_/output/Normal"));

		job.setMapOutputKeyClass(Mean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

//used to compute mean value of rating and user_income
class Mean implements Writable, WritableComparable<Object>{
	private DoubleWritable rating;
	private DoubleWritable income;
	private IntWritable count;

	public Mean() {
		rating = new DoubleWritable(0);
		income = new DoubleWritable(0);
		count = new IntWritable(0);
	}

	/*
	 * add a line's value to result
	 */
	public void addTuple(double rating, double income) {
		this.rating.set(rating + this.rating.get());
		this.income.set(income + this.income.get());
		this.count.set(this.count.get() + 1);
	}

	public DoubleWritable getRating() {
		return rating;
	}

	public void setRating(DoubleWritable rating) {
		this.rating = rating;
	}

	public DoubleWritable getIncome() {
		return income;
	}

	public void setIncome(DoubleWritable income) {
		this.income = income;
	}

	public IntWritable getCount() {
		return count;
	}

	/*
	 * get rating's mean value
	 */
	public double rating_mean() {
		return rating.get()/count.get();
	}
	
	/*
	 * get income's mean value
	 */
	public double income_mean() {
		return income.get()/count.get();
	}
	
	public void add(Mean mean) {
		this.income.set(this.income.get() + mean.getIncome().get());;
		this.rating.set(this.rating.get() + mean.getRating().get());;
		this.count.set(mean.getCount().get() + this.count.get());
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		rating.readFields(arg0);
		income.readFields(arg0);
		count.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		rating.write(arg0);
		income.write(arg0);
		count.write(arg0);
	}
	
	@Override
	public int compareTo(Object o) {
		Mean m = (Mean)o;
		if(this.income.get() < m.getIncome().get()) {
			return -1;
		}else {
			return 1;
		}
	}
	
}
