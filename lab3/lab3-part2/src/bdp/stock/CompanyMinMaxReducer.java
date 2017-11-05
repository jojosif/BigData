package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

public class CompanyMinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
              throws IOException, InterruptedException {
        
        double min = 99999999999999999999999.9;
        double max = 0.0;
		
        for (DoubleWritable value : values) {
            if(value.get() >  max){
                    max = value.get();
            }
            if(value.get() < min){
                    min = value.get();
            }
        }     
        String print = ", MIN: " + min + "  MAX: " + max;

        context.write(key, new Text(print));
		
    }
}
