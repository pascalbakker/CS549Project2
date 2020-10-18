package edu.wpi.project2;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataOutput;
import java.io.DataInput;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
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

public class Outlier {

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

    public PointWritable(PointWritable Point) {
      x = new DoubleWritable(Point.getx());
      y = new DoubleWritable(Point.gety());
      count = new IntWritable(Point.getc());
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

    public double distanceTo(PointWritable Z) {
      double dx = Math.pow(this.getx() - Z.getx(), 2);
      double dy = Math.pow(this.gety() - Z.gety(), 2);
      return Math.sqrt(dx + dy);
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

  public static class OMapper extends Mapper<Object, Text, Text, PointWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      double radius = Double.parseDouble(conf.get("Radius"));

      //Define objects based on reading each line
      String[] arrOfStr = value.toString().split(",");
      double x = Double.parseDouble(arrOfStr[0]);
      double y = Double.parseDouble(arrOfStr[1]);

      int loc_x = (int)(x/radius);
      int loc_y = (int)(y/radius);

      PointWritable Point_in = new PointWritable(x, y, 1000);
      PointWritable Point_out = new PointWritable(x, y, -1);

      Text location;

      for (int i = loc_x - 1; i <= loc_x + 1; i++) {
        for (int j = loc_y -  1; j <= loc_y + 1; j++) {
          location = new Text();
          location.set(i +","+ j);
          if (i==loc_x && j==loc_y) {
            context.write(location, Point_in);
          } else {
            context.write(location, Point_out);
          }
        }
      }


    }
  }

  public static class OReducer extends Reducer<Text, PointWritable, NullWritable, PointWritable> {


    public void reduce(Text key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {

      int id;
      int counter;
      double distance;
      List<PointWritable> Points = new ArrayList<PointWritable>();
      List<PointWritable> Neighbors = new ArrayList<PointWritable>();
      Configuration conf = context.getConfiguration();
      double radius = Double.parseDouble(conf.get("Radius"));
      int k_n = Integer.parseInt(conf.get("Neighbors"));

      for (PointWritable point_loc : values) {
        PointWritable writable = new PointWritable(point_loc);
        Points.add(writable);
      }

      for (PointWritable point_loc : Points) {
        PointWritable writable = new PointWritable(point_loc);
        Neighbors.add(writable);
      }

      for (PointWritable point : Points) {
        id = point.getc();
        if (id > 0) {
          //context.write(NullWritable.get(), point);
          counter = 0;

          for (PointWritable neighbor : Neighbors) {
            distance = point.distanceTo(neighbor);
            if (distance < radius) {
              counter ++;
            }
          }
          counter --;
          if (counter < k_n) {
            context.write(NullWritable.get(), point);
          }

        }
      }
    }
  }




  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.set("Radius", args[2]);
    conf.set("Neighbors", args[3]);


    Job job = Job.getInstance(conf, "Outlier");
    job.setJarByClass(Outlier.class);
    job.setMapperClass(OMapper.class);
    job.setReducerClass(OReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PointWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(PointWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);


  }
}
