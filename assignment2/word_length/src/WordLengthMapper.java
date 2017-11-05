import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordLengthMapper extends
		Mapper<Object, Text, IntWritable, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private final IntWritable length = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Use output of part 2
		// Split by tab spacing
		String[] line = value.toString().split("\t");
		length.set(line[0].length());
		context.write(length, one);

	}
}
