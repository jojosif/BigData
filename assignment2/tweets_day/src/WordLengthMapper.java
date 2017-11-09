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

        if(value.toString().split(";").length == 4) {

            String[] line = value.toString().split(";");
            LocalDateTime.ofEpochSecond(line[0]/1000,
            Instant t = Instant.ofEpochMilli(line[0]);
            ZoneDateTime d = ZoneDateTime.ofInstant(t,ZoneId.SystemDefault());
            lengthset(d.gethour());
            context.write(length, one);


            }
        }
    }

