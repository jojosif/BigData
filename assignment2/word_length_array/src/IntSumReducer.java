import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
	private IntWritable result = new IntWritable();
   	int[] data;
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
 
    	int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
               result.set(sum);
        
        //context.write(key, result);
        
        data = new int[sum % 5]{1}

        context.write(data)
    }
}
