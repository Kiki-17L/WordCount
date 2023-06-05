package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.mapper.WordCountMapper;
import org.example.reducer.WordCountReducer;


import java.io.IOException;

public class WordCountMaster {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        System.setProperty("hadoop.home.dir","D://ProgramFiles/hadoop-3.2.4");


        Configuration conf= new Configuration();

        Job job= Job.getInstance(conf,"WordCount");


        job.setJarByClass(WordCountMaster.class);

        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);


        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean result=job.waitForCompletion(true);

        if (result){

            System.out.println("Congratulations!");
        }

    }
}