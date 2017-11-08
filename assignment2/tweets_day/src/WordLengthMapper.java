import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordLengthMapper extends
        Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable length = new IntWritable(1);
    private final IntWritable tir = new IntWritable(1);
    private final Text data = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(value.toString().split(";").length == 4) {

            String[] line = value.toString().split(";");
            LocalDateTime epoch = LocalDateTime.ofEpochSecond(Long.parseLong(line[0])/1000, 0, ZoneOffset.UTC);
            DateTimeFormatter f = DateTimeFormatter.ISO_LOCAL_TIME();
            time = epoch.format(f);
            data.set(time);

            length.set(line[2].length());
            double number = line[2].length() ;

            if (number < 141){

                context.write(time, one);


            }
        }
    }
}
