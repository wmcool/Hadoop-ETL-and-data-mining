package preprocessing;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * 1.sample on user_career
 * 2.transfer user_birthday and review_date to yyyy-MM-dd
 * 3.filter on longitude and latitude
 */
public class Sample_Filter {
	
    private static SimpleDateFormat df1 = new SimpleDateFormat("MMMMM dd,yyyy", Locale.ENGLISH);
    private static SimpleDateFormat df2 = new SimpleDateFormat("yyyy/MM/dd");
    private static SimpleDateFormat df3 = new SimpleDateFormat("yyyy-MM-dd");
    
	public static class SampleMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] attr = line.split("\\|");
			
			//attract user_career
			String user_career = attr[10];
			
			context.write(new Text(user_career), value);
		}
	}

	public static class SampleReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private Date birthday = null;
		private Date review_date = null;
		private static DecimalFormat df = new DecimalFormat("#0.0");
		
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int base = RandomUtils.nextInt(0,6);
			int count = 0;
			StringBuilder sampleBuilder = new StringBuilder();
			StringBuilder filterBuilder = new StringBuilder();
			for(Text text:values) {
				//sample
				if((count - base) % 5 == 0) {
					String line = text.toString();
					String[] attr = line.split("\\|");
					
					//transfer review_date to yyyy-MM-dd
					String reviewDate = attr[4];
					if(reviewDate.contains("/")) {
						try {
							review_date = df2.parse(reviewDate);
							attr[4] = df3.format(review_date);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else if(!reviewDate.contains("-")){
						try {
							review_date = df1.parse(reviewDate);
							attr[4] = df3.format(review_date);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					//transfer birthday format to yyyy-MM-dd
					String user_birthday = attr[8];
					if(user_birthday.contains("/")) {
						try {
							birthday = df2.parse(user_birthday);
							attr[8] = df3.format(birthday);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else if(!user_birthday.contains("-")){
						try {
							birthday = df1.parse(user_birthday);
							attr[8] = df3.format(birthday);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					//transfer temperature to Centigrade
					String temperature = attr[5];
					if(temperature.contains("℉")) {
						double Fah = Double.parseDouble(temperature.substring(0, temperature.length()-2));
						double Cen = (Fah - 32) / 1.8;
						temperature = df.format(Cen) + "℃";
						attr[5] = temperature;
					}
					
					line = StringUtils.join(attr, '|');
					sampleBuilder.append(line + "\n");
					
					double longtitude = Double.parseDouble(attr[1]);
					double latitude = Double.parseDouble(attr[2]);
					//filter
					if(longtitude >= 8.1461259 && longtitude <= 11.1993265 &&
							latitude >= 56.5824856 && latitude <= 57.750511) {
						filterBuilder.append(line + "\n");
					}
				}
				count++;
			}
			multipleOutputs.write(new Text(sampleBuilder.toString()), NullWritable.get(), "D_Sample");
			multipleOutputs.write(new Text(filterBuilder.toString()), NullWritable.get(), "D_Filter");
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
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
		job.setJarByClass(Sample_Filter.class);
		job.setJobName("Sample && filter");
		
		job.setMapperClass(SampleMapper.class);
	    job.setReducerClass(SampleReducer.class);
	    
		FileInputFormat.addInputPath(job, new Path("/user/hduser_/input"));
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path("/user/hduser_/output/Sample_Filter"))) {
			fs.delete(new Path("/user/hduser_/output/Sample_Filter"), true);
		}
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser_/output/Sample_Filter"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(MyFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MyFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class MyFileOutputFormat extends TextOutputFormat<Text,Text>{
	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
		// TODO Auto-generated method stub
		FileOutputCommitter fileOutputCommitter=(FileOutputCommitter)getOutputCommitter(context);
		return new Path(fileOutputCommitter.getWorkPath(), getOutputName(context));
	}
}
