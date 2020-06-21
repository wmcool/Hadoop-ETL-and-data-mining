package naiveBayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// compute means of every class's every dimension
public class Means {
	public static class MeansMapper extends Mapper<Object, Text, IntWritable, Text>{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int label = line.charAt(line.length()-1) - '0';// get label of data
			context.write(new IntWritable(label), value);// <label, data>
		}
		
	}
	
	public static class MeansReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		private MultipleOutputs<Text, NullWritable> multipleOutputs;
		private Configuration conf;
		private int num_attrs; // number of attributes(not including class label)
		private int num_labels;// number of labels
		private int[] class_prior;// compute class prior

		@Override
		protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			conf = context.getConfiguration();
			multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
			num_attrs = conf.getInt("num_attrs", 20);
			num_labels = conf.getInt("num_labels", 2);
			class_prior = new int[num_labels];
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			int label = key.get();
			int count = 0;
			double[] means = new double[num_attrs];
			for(Text line:values) {
				String[] attrs = line.toString().split(",");
				for(int i=0;i<num_attrs;i++) {
					means[i] += Double.parseDouble(attrs[i]);// sum up every dimension's value
				}
				count++;
			}
			class_prior[label] = count;
			for(int i=0;i<num_attrs;i++) {
				means[i] /= count; // compute mean value of every dimension
				multipleOutputs.write(new Text(label + "," + i + "," + means[i]), NullWritable.get(),"means.txt");
			}
		}

		@Override
		protected void cleanup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int sumCounts = 0;
			for(int i=0;i<num_labels;i++) {
				sumCounts += class_prior[i];// compute numbers of all data
			}
			for(int j=0;j<num_labels;j++) {
				double prior = class_prior[j] / (double)sumCounts;// compute class prior
				multipleOutputs.write(new Text(j + "," + prior), NullWritable.get(), "class_prior.txt");
			}
			multipleOutputs.close();
		}
		
	}
}
