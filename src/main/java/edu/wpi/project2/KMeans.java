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



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    int iteration = 0;
    boolean unequal = true;

    while (iteration < 6 && unequal) {

      // Define file for centroids
      if (iteration == 0) {
        conf.set("Centroids", args[2]);
      } else {
        conf.set("Centroids", args[1] +"/"+ String.valueOf(iteration)+"/part-r-00000");
      }

      Job job = Job.getInstance(conf, "KMeans");
      job.setJarByClass(KMeans.class);
      job.setMapperClass(KMapper.class);
      job.setCombinerClass(KCombiner.class);
      job.setNumReduceTasks(1);
      job.setReducerClass(KReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(PointWritable.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(PointWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1] +"/"+ String.valueOf(iteration+1)));
      job.waitForCompletion(true);

      iteration ++;

      //Compare given centroids with new centroids
      Path path_before = new Path(conf.get("Centroids"));
      FileSystem fs_before = FileSystem.get(path_before.toUri(), conf);
      BufferedReader br_before = new BufferedReader(new InputStreamReader(fs_before.open(path_before)));

      Path path_after = new Path(args[1] +"/"+ String.valueOf(iteration)+"/part-r-00000");
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
}
