package edu.wpi.project2;
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

public class KMeans {

  public static class PointWritable implements Writable {
    private DoubleWritable x;
    private DoubleWritable y;
    private IntWritable count;

    public PointWritable() {
      x = new DoubleWritable();
      y = new DoubleWritable();
      count = new IntWritable();
    }

    public PointWritable(double X_, double Y_, int N_) {
      x = new DoubleWritable(X_);
      y = new DoubleWritable(Y_);
      count = new IntWritable(N_);
    }

    public double getx() {
      return this.x.get();
    }

    public double gety() {
      return this.y.get();
    }

    public int getc() {
      return this.count.get();
    }

    public double distanceTo(double X_, double Y_) {
      double dx = Math.pow(this.getx() - X_, 2);
      double dy = Math.pow(this.gety() - Y_, 2);
      return Math.pow(dx + dy, 0.5);
    }

    @Override
    public String toString() {
      return x.toString() +","+ y.toString();
      }

    @Override
    public void readFields(DataInput in) throws IOException {
      x.readFields(in);
      y.readFields(in);
      count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      x.write(out);
      y.write(out);
      count.write(out);
    }
  }

  public static class KMapper extends Mapper<Object, Text, Text, PointWritable>{

    //Define variables
    private List<Double> cX = new ArrayList<Double>();
    private List<Double> cY = new ArrayList<Double>();
    private int cluster_i;

    public void setup(Context context) throws IOException {


      Configuration conf = context.getConfiguration();
      String centroids = conf.get("Centroids");
      Path path = new Path(centroids);
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      //Add points at 0
      cX.add(0.0);
      cY.add(0.0);

      try {
        String line = br.readLine();
        while (line != null){
          String[] arrOfStr = line.split(",");
          cX.add(Double.valueOf(arrOfStr[0]));
          cY.add(Double.valueOf(arrOfStr[1]));
          line = br.readLine();
        }
      } finally {
        br.close();
      }

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      //Define objects based on reading each line
      String[] arrOfStr = value.toString().split(",");
      PointWritable Point = new PointWritable(Double.parseDouble(arrOfStr[0]), Double.parseDouble(arrOfStr[1]), 1);
      double min_distance = Double.MAX_VALUE;

      //Select closer cluster
      for(int i = 1; i < cX.size(); i++){
        if(Point.distanceTo(cX.get(i), cY.get(i)) < min_distance){
          min_distance = Point.distanceTo(cX.get(i), cY.get(i));
          cluster_i = i;
        }
      }

      //Define Key and Write
      Text cluster_string = new Text();
      String string_key = String.valueOf(cX.get(cluster_i)) +','+ String.valueOf(cY.get(cluster_i));
      cluster_string.set(string_key);
      context.write(cluster_string, Point);

    }
  }

  public static class KCombiner extends Reducer<Text, PointWritable, Text, PointWritable> {

    public void reduce(Text key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
      double x = 0;
      double y = 0;
      int count = 0;

      for (PointWritable point : values) {
        x += point.getx();
        y += point.gety();
        count += point.getc();
      }

      PointWritable Point = new PointWritable(x, y, count);
      context.write(key, Point);
    }
  }

  public static class KReducer extends Reducer<Text, PointWritable, NullWritable, PointWritable> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
      double x = 0;
      double y = 0;
      int count = 0;

      for (PointWritable point : values) {
        x += point.getx();
        y += point.gety();
        count += point.getc();
      }

      PointWritable Point = new PointWritable(x/count, y/count, 1);
      result.set(Point.toString());
      context.write(NullWritable.get(), Point);
    }
  }
}
