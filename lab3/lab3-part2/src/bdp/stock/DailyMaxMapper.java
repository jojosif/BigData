package bdp.stock;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class DailyMaxMapper extends Mapper<Text, DailyStock, Text, DoubleWritable> { 

    public void map(Text key, DailyStock stock, Context context) throws IOException, InterruptedException {


        context.write(stock.getCompany(),stock.getHigh());
        context.write(stock.getCompany(), stock.getLow());
        
    
    }
}
