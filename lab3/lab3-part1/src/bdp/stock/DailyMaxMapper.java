package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyMaxMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
  
    private IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      
	String[] fields = line.toString().split(",");  
        
        //Fields contains line as follows. 
        //   0         1           2      3    ...........
        //exchange, stock_symbol, date, stock_price_open,stock_price_high,stock_price_low, stock_price_close, stock_volume,stock_price_adj_close.
        
        context.write(new Text(fields[1]), one);
        
    
    }
}
