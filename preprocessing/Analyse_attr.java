package preprocessing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

//compute rating's max and min

public class Analyse_attr {
	
	public static class AnalyseMapper extends Mapper<Object, Text, Tuple, NullWritable> {
		private Tuple maxmin = new Tuple();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if(line.length() <= 1) return;//filter \n
			String attr = line.split("\\|")[6];
			
			if(attr.equals("?")) {//missing value
				return;
			}
			
			//attract rating
			double rating = Double.parseDouble(attr);
			
			//get min
			if(rating < maxmin.getMin()) {
				maxmin.setMin(rating);
			}
			
			//get max
			if(rating > maxmin.getMax()) {
				maxmin.setMax(rating);
			}
		}
		@Override 
		protected void cleanup(Context context) throws IOException, 
		InterruptedException { 
			context.write(maxmin, NullWritable.get()); //write max and min to Reducer
		}
	}
	
public static class AnalyseReducer extends Reducer<Tuple, NullWritable, Text, NullWritable> {
		
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private Tuple result = new Tuple();
		
		@Override
		protected void setup(Reducer<Tuple, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
		}
		
		@Override
		protected void reduce(Tuple key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			//get min
			if(key.getMin() < result.getMin()) {
				result.setMin(key.getMin());
			}
			
			//get max
			if(key.getMax() > result.getMax()) {
				result.setMax(key.getMax());
			}
		}
		
		@Override
		protected void cleanup(Reducer<Tuple, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			multipleOutputs.write(new Text(result.getMax() + " " + result.getMin()), NullWritable.get(), "Max_Min");
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
		job.setJarByClass(Analyse_attr.class);
		job.setJobName("Analyse attribute");
		
		job.setMapperClass(AnalyseMapper.class);
	    job.setReducerClass(AnalyseReducer.class);
	    
		FileInputFormat.addInputPath(job, new Path("/user/hduser_/output/Sample_Filter/D_Filter"));
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path("/user/hduser_/output/Analyse_attr"))) {
			fs.delete(new Path("/user/hduser_/output/Analyse_attr"), true);
		}
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser_/output/Analyse_attr"));

		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

// used to storage max value and min value
class Tuple implements Writable, WritableComparable<Object>{
	private DoubleWritable max;
	private DoubleWritable min;
	
	Tuple(){
		max = new DoubleWritable(Double.MIN_VALUE);
		min = new DoubleWritable(Double.MAX_VALUE);
	}
	public double getMax() {
		return max.get();
	}

	public void setMax(double max) {
		this.max = new DoubleWritable(max);
	}

	public double getMin() {
		return min.get();
	}

	public void setMin(double min) {
		this.min = new DoubleWritable(min);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		min.readFields(arg0);
		max.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		min.write(arg0);
		max.write(arg0);
	}
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Tuple t = (Tuple)o;
		if(this.max.get() < t.getMax()) {
			return -1;
		}else if(this.max.get() > t.getMax()) {
			return 1;
		}else {
			return 0;
		}
	}
	
}
