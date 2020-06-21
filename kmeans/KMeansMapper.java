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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Point;

public class KMeansMapper extends Mapper<Object, Text, Point, Point>{
	
	private ArrayList<Point> centroids;
	
	@Override
	protected void setup(Mapper<Object, Text, Point, Point>.Context context)
			throws IOException, InterruptedException {
		FileSystem fs = null;
		centroids = new ArrayList<>();
		Configuration conf = context.getConfiguration();
		try {
			fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		} catch (Exception e) {

		}
		FSDataInputStream fi = fs.open(new Path("hdfs://localhost:9000/" + conf.get("centroid_path")));// read the centroids from hdfs
		BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
		String line;
		while((line = bf.readLine()) != null){
			centroids.add(Point.parse(line));
		}
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Point, Point>.Context context)
			throws IOException, InterruptedException {
		Point point = Point.parse(value.toString());// transform a line of data to a point in n-dimensional space
		int index = 0;
		double minDistance = Double.MAX_VALUE;
		for(Point centroid:centroids) {// find the nearest centroid to the point
			double distance = point.getEuclDistance(centroid);
			if(distance < minDistance) {
				minDistance = distance;
				index = centroids.indexOf(centroid);
			}
		}
		context.write(centroids.get(index), point);// <centroid, point>
	}

	
}
