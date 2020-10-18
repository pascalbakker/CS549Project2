package edu.wpi.project2;
import edu.wpi.project2.Outlier;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataOutput;
import java.io.DataInput;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import edu.wpi.project2.Problem3JsonFormatting;
import edu.wpi.project2.Problem3CustomMapper;
import edu.wpi.project2.Problem3CustomReducer;
import edu.wpi.project2.Outlier.PointWritable;
import edu.wpi.project2.KMeans;
import edu.wpi.project2.Outlier.OMapper;
import edu.wpi.project2.Outlier.OReducer;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
        //problem1(pointCSVPath, rectangleCSVPath, outputCSVPath);
        problem3("data/airfield.json", "data/results/problem3");
        /*
        if(args.length==0) return;
        switch(args[0]){
            //case "1": problem1(pointCSVPath, rectangleCSVPath, outputCSVPath);
            //case "2": problem2("data/problem2csv", "data/results/problem2/", "data/centroids.csv");
			//case "3": problem3("data/airfield.json", "data/results/problem3");
			//case "4": problem4("data/airfield.json", "data/results/problem3");
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
        Job job = Job.getInstance(conf, "Problem 1");
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

    public static void problem2(String inputPath, String outputPath, String centroidPath) throws Exception{
    Configuration conf = new Configuration();

    int iteration = 0;
    boolean unequal = true;

    while (iteration < 6 && unequal) {

      // Define file for centroids
      if (iteration == 0) {
        conf.set("Centroids", centroidPath);
      } else {
        conf.set("Centroids", outputPath +"/"+ String.valueOf(iteration)+"/part-r-00000");
      }

      Job job = Job.getInstance(conf, "KMeans");
      job.setJarByClass(KMeans.class);
      job.setMapperClass(KMeans.KMapper.class);
      job.setCombinerClass(KMeans.KCombiner.class);
      job.setNumReduceTasks(1);
      job.setReducerClass(KMeans.KReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(KMeans.PointWritable.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(KMeans.PointWritable.class);

      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath +"/"+ String.valueOf(iteration+1)));
      job.waitForCompletion(true);

      iteration ++;

      //Compare given centroids with new centroids
      Path path_before = new Path(conf.get("Centroids"));
      FileSystem fs_before = FileSystem.get(path_before.toUri(), conf);
      BufferedReader br_before = new BufferedReader(new InputStreamReader(fs_before.open(path_before)));

      Path path_after = new Path(outputPath +"/"+ String.valueOf(iteration)+"/part-r-00000");
      FileSystem fs_after = FileSystem.get(path_after.toUri(), conf);
      BufferedReader br_after = new BufferedReader(new InputStreamReader(fs_after.open(path_after)));


      //System.out.println("Iteration Number " + String.valueOf(iteration) );
      int aux = 0;
      int equals = 0;

      try {
        String line = br_before.readLine();
        String line_2 = br_after.readLine();
        while (line != null && line_2 != null){
          aux ++;
          //System.out.println(line +" == "+ line_2 +" == "+ String.valueOf(line.equals(line_2)));
          if (line.equals(line_2)) {
            equals++;
          }
          line = br_before.readLine();
          line_2 = br_after.readLine();
        }
      } finally {
        br_before.close();
        br_after.close();
      }
      if (aux == equals) {
        unequal = false;
      }
      }
    }
    public static void problem3(String inputPath, String outputPath) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);
        Job job = Job.getInstance(conf, "Problem3JsonFormatting");
        job.setJarByClass(Problem3JsonFormatting.class);
        job.setMapperClass(Problem3JsonFormatting.CustomMapper.class);
        job.setReducerClass(Problem3JsonFormatting.CustomReducer.class);
        job.setInputFormatClass(Problem3JsonFormatting.JSONInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Problem3JsonFormatting.JSONInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean jobStatus = job.waitForCompletion(true);
        System.exit(jobStatus ? 0 : 1);
    }

  public static void problem4(String inputPath, String outputPath, String radius, String kneighbors) throws Exception {

    Configuration conf = new Configuration();
    conf.set("Radius", radius);
    conf.set("Neighbors", kneighbors);


    Job job = Job.getInstance(conf, "Outlier");
    job.setJarByClass(Outlier.class);
    job.setMapperClass(Outlier.OMapper.class);
    job.setReducerClass(Outlier.OReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Outlier.PointWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Outlier.PointWritable.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.waitForCompletion(true);
  }
}
