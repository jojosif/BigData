import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordLengthMapper extends
		Mapper<Object, Text, DoubleWritable, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private final IntWritable length = new IntWritable(1);
    private final DoubleWritable tir = new DoubleWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
		if(value.toString().split(";").length == 4) {
		
			String[] line = value.toString().split(";");	
			length.set(line[2].length());	 
			double number = line[2].length() ;
				if (number < 140){
				    double size = (number % 5);
				    tir.set(size);
					context.write(tir, one);
					

	}
}
}
}