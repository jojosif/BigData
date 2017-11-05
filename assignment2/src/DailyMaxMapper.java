package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyMaxMapper extends Mapper<Object, Text, Text, IntWritable> { 
  
    private IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
      
	String[] fields = line.toString().split(";");  
        
        context.write(new Text(fields[3]), one);
        
    
    }
}
