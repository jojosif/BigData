package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanyMinMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public IntWritable count = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {     

        int length = 0;
        for(IntWritable partialCount: values){
            sum +=partialCount.length();
        }
        
        count.set(sum);
        
        
        context.write(key, count);
		
    }
}
