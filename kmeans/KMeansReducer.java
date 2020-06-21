package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Point;

public class KMeansReducer extends Reducer<Point, Point, Text, NullWritable>{
	
	private double delta = 0.;
	
	
	@Override
	protected void setup(Reducer<Point, Point, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
        delta = conf.getDouble("delta", 0.1);
	}


	@Override
	protected void reduce(Point key, Iterable<Point> values,
			Reducer<Point, Point, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		Point newCentroid = new Point();
		int i = 0;
		for(Point point:values) { // sum up all points belong to centroid_k
			newCentroid.add(point);
			i++;
		}
		newCentroid.divide(i);// compute means of all points
		System.out.println(newCentroid.getEuclDistance(key));
		if(newCentroid.getEuclDistance(key) > delta) { // if error > delta, increment counter
			context.getCounter(KmeansCounter.CONVERGED).increment(1L);
		}
		context.write(new Text(newCentroid.toString()), NullWritable.get());// <new_centroid, null>
	}
	
}
