import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
package com.twitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;



public class WordLengthMapper extends
        Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable length = new IntWritable(1);
    private final IntWritable erro = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {


    	if(value.toString().split(";").length == 4) {

      // try {

        	String[] line = value.toString().split(";");


        	String [] x = line[2]
            Pattern MY_PATTERN = Pattern.compile("#(\\w+)");
            Matcher mat = MY_PATTERN.matcher(x);
            while (mat.find()) {
                context.write(mat, one);
            }



            //}
    //    }  catch (NumberFormatException e){

      //  	erro.set(27);
        //	context.write(erro, one);


        }
        }
    }
}







