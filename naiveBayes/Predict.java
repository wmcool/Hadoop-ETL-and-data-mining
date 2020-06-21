package naiveBayes;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import utils.Tools;

//predict data's label using mu and sigma
public class Predict {
	public static class PredictMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		private Configuration conf;
		private int num_attrs;
		private int num_labels;
		private double[][] means;
		private double[][] vars;
		private double[] class_prior;
		private boolean compute_loss;// if compute_loss == true, <map> will get the label(if exists) and compute accuracy rate
		private int correct = 0;// correctly predict numbers of data
		private int count = 0;

		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			
			num_attrs = conf.getInt("num_attrs", 20);
			num_labels = conf.getInt("num_labels", 2);
			compute_loss = conf.getBoolean("compute_loss", false);
			
			means = new double[num_labels][num_attrs];
			vars = new double[num_labels][num_attrs];
			class_prior = new double[num_labels];
			
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (Exception e) {

			}
			
			// read means value from hdfs
			FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/naive_bayes/compute_means/means.txt"));
			BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
			String line;
			while((line = bf.readLine()) != null) {
				String[] attrs = line.split(",");
				int label = Integer.parseInt(attrs[0]);
				int attr = Integer.parseInt(attrs[1]);
				double mean = Double.parseDouble(attrs[2]);
				means[label][attr] = mean;
			}
			
			// read variance from hdfs
			fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/naive_bayes/compute_var/variances.txt"));
			bf = new BufferedReader(new InputStreamReader(fi));
			while((line = bf.readLine()) != null) {
				String[] attrs = line.split(",");
				int label = Integer.parseInt(attrs[0]);
				int attr = Integer.parseInt(attrs[1]);
				double var = Double.parseDouble(attrs[2]);
				vars[label][attr] = var;
			}
			
			// read class prior from hdfs
			fi = fs.open(new Path("hdfs://localhost:9000/user/lab2/output/naive_bayes/compute_means/class_prior.txt"));
			bf = new BufferedReader(new InputStreamReader(fi));
			while((line = bf.readLine()) != null) {
				String[] attrs = line.split(",");
				int label = Integer.parseInt(attrs[0]);
				double prior = Double.parseDouble(attrs[1]);
				class_prior[label] = prior;
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] attrs = value.toString().split(",");
			double[] posterior = new double[num_labels];// posterior probability(omit p(x))
			double[] real_attrs = new double[num_attrs];
			
			for(int i=0;i<num_labels;i++) {
				posterior[i] = class_prior[i];// initialize posterior
			}
			
			for(int i=0;i<num_attrs;i++) {
				real_attrs[i] = Double.parseDouble(attrs[i]);// every dimension's value of data
			}
			
			for(int i=0;i<num_labels;i++) {
				for(int j=0;j<num_attrs;j++) {
					posterior[i] *= Tools.Gaussian(real_attrs[j], means[i][j], vars[i][j]);// compute posterior probability
				}
			}
			
			int predict_label = 0;
			double max = Double.MIN_VALUE;
			
			//choose the max posterior probability
			for(int i=0;i<num_labels;i++) {
				if(posterior[i]>max) {
					predict_label = i;
					max = posterior[i];
				}
			}
			
			if(compute_loss) {// compute cost and accuracy
				if(Integer.parseInt(attrs[num_attrs]) == predict_label) {
					correct++;
				}
				count++;
			}
//			context.write(key, new Text(value.toString() + " " + predict_label));
			context.write(key, new Text(predict_label + ""));
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
				multipleOutputs.write(line, NullWritable.get(), "1170100513_3.txt");// write data in order
			}
		}
	}
	
}
