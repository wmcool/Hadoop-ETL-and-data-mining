package utils;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyFileOutputFormat extends TextOutputFormat<Text,Text>{
	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
		// TODO Auto-generated method stub
		FileOutputCommitter fileOutputCommitter=(FileOutputCommitter)getOutputCommitter(context);
		return new Path(fileOutputCommitter.getWorkPath(), getOutputName(context));
	}
}
