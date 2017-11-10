// mapper

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetNameTable extends Mapper<LongWritable, Text, Text, IntWritable> { 
  
    private IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      
	String[] fields = line.toString().split(",");      
    context.write(new Text(fields[1]), one);
        
    
    }
}

//reducer

public class countname extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public IntWritable count = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {     

        int sum = 0;
        for(IntWritable partialCount: values){
            sum +=partialCount.get();
        }
        
        count.set(sum);
        
        
        context.write(key, count);
		
    }
}

// drive

public class CountCompany {
    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(GetNameTable.class);
        job.setReducerClass(countname.class);
        job.setMapperClass(GetNameTable.class);
   
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
        }
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}