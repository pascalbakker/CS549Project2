package edu.wpi.project2;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import edu.wpi.project2.Problem1.PointDataMapper;
import edu.wpi.project2.Problem1.RectangleDataMapper;
import edu.wpi.project2.Problem1.SpatialReducer;

/**
 * Project 1
 *
 */
public class App {
    public static void main( String[] args ) throws Exception
    {
        // properties set so that pig scripts can be run
        Properties props = new Properties();
        props.setProperty("fs.default.name", "hdfs://localhost:9000");
        //props.setProperty("fs.default.name", "hdfs://<namenode-hostname>:<port>");
        //props.setProperty("mapred.job.tracker", "<jobtracker-hostname>:<port>");
        org.apache.log4j.BasicConfigurator.configure(); // log output
        // Problem 1 Paths
        String pointCSVPath = "data/PointData.csv";
        String rectangleCSVPath = "data/RectangleData.csv";
        String outputCSVPath = "data/results/Problem1/";
        // run hadoop job
        problem1(pointCSVPath, rectangleCSVPath, outputCSVPath);
        /*
        if(args.length==0) return;
        switch(args[0]){
            //case "1": mr_query1(customerPath,transactionPath,"data/result/query2");
            //case "2": mr_query2(transactionPath,"data/result/query3");
            //case "3": mr_query3(customerPath,transactionPath,"data/result/query4");
            //case "4": mr_query4(transactionPath,"data/result/query5");
            default: System.out.println("Arguements not valid. Please pass a number between 2 and 5");
        }
        */
    }

    public static void deleteResults(String result_path){
            File resultsFolder = new File(result_path);
            resultsFolder.delete();
    }

    public static void problem1(String pointCSVPath, String rectangleCSVPath, String outputCSVPath)throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Problem 1 SpacialJoin");
        job.setJarByClass(Problem1.class);
        MultipleInputs.addInputPath(job, new Path(pointCSVPath), TextInputFormat.class, PointDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(rectangleCSVPath), TextInputFormat.class, RectangleDataMapper.class);
        job.setReducerClass(SpatialReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputCSVPath));
        boolean jobStatus = job.waitForCompletion(true);
        System.exit(jobStatus ? 0 : 1);
    }
}
