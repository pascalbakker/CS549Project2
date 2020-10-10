package edu.wpi.project2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// Spatial Join of rectangles and points
public class Problem1{
	public static final double SpaceMaxX = 10000;
	public static final double SpaceMaxY = 10000;
	public static final double SpaceMinX = 0;
	public static final double SpaceMinY = 0;
	public static final int subspaceRows = 3;
	public static final int subspaceCols = 3;
	public static final List<RectangleData> subspaces = createSubSpaces();

	public static boolean isInWindowPoint(RectangleData w,PointData p){
		return p.x >= w.x && p.y >=w.y && p.x <= w.x+w.width && p.x <= w.y+w.height;
	}

	public static boolean isInWindow(RectangleData w,RectangleData r){
		return r.x >= w.x && r.y >=w.y && r.x+r.width<=w.x+w.width && r.y+r.height <= w.y+w.height;
	}

	// Note: isInSubspace for Pointdata is same as isInWindow for Point Data
	public static boolean isInSubspace(RectangleData s, RectangleData r){
		return (r.x >= s.x && r.x+r.width<=s.x+s.width) || (r.y >= s.y && r.y+r.height<=s.y+s.height);
	}

	// creats grid of subspaces
	public static ArrayList<RectangleData> createSubSpaces(){
		// Key is subspace. row = index / subspaceCols col = index % subspaceRows
		ArrayList<RectangleData> subspaces = new ArrayList<RectangleData>();
		final double subspaceSizeY = (SpaceMaxY-SpaceMinY)/subspaceRows;
		final double subspaceSizeX = (SpaceMaxX-SpaceMinX)/subspaceCols;
		// For Point Data
		for(int i=0;i<subspaceCols;i++){
			double bottom_x = i*subspaceSizeX;
			for(int j = 0;j<subspaceRows;j++){
				double bottom_y = j*subspaceSizeY;
				subspaces.add(new RectangleData(bottom_x,bottom_y,bottom_x+subspaceSizeX,bottom_y+subspaceSizeY));
			}
		}
		return subspaces;
	}

	// PointData x,y
	// Rectangle Data bottom_x,bottom_y,height,width
	// maps rectangles to subspace.
	public static class RectangleDataMapper extends Mapper<LongWritable, Text,IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try{
				String[] v = value.toString().split(",");
				RectangleData r = new RectangleData(Double.parseDouble(v[0]),Double.parseDouble(v[1]),Double.parseDouble(v[2]),Double.parseDouble(v[3]));
				int groupIndex = 0;
				for(RectangleData s: subspaces){
					if(isInSubspace(s, r)){
						context.write(new IntWritable(groupIndex), new Text("R~"+r.toString()));
						break;
					}
					groupIndex++;
				}

			} catch(Exception ex){
					ex.printStackTrace();
					System.exit(1);
			}
		}
	}

	// Maps points to subspace
	public static class PointDataMapper extends Mapper<LongWritable, Text,IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try{
				String[] v = value.toString().split(",");
				PointData p = new PointData(Double.parseDouble(v[0]),Double.parseDouble(v[1]));
				int groupIndex = 0;
				for(RectangleData s: subspaces){
					if(isInWindowPoint(s, p)){
						context.write(new IntWritable(groupIndex), new Text("P~"+p.toString()));
						break;
					}
					groupIndex++;
				}

			} catch(Exception ex){
					ex.printStackTrace();
					System.exit(1);
			}
		}
	}

	// Determines which points are in which triangles in each subspace
	public static class SpatialReducer extends Reducer<IntWritable,Text,Text,Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			List<PointData> points = new ArrayList<PointData>();
			List<RectangleData> rectangles = new ArrayList<RectangleData>();
			try{
				// Get all points and rectangles in subspace
				for(Text value: values){
					String[] dataValue = value.toString().split("~");
					if(dataValue[0].equals("P")){
						String[] pValues = dataValue[1].split(",");
						points.add(new PointData(Double.parseDouble(pValues[0]),Double.parseDouble(pValues[1])));
					}else if(dataValue[0].equals("R")){
						String[] rValues = dataValue[1].split(",");
						rectangles.add(new RectangleData(Double.parseDouble(rValues[0]),Double.parseDouble(rValues[1]),Double.parseDouble(rValues[2]),Double.parseDouble(rValues[3])));
					}
				}
				// Find if point is in rectangle
				for(PointData p: points){
						for(RectangleData r: rectangles){
							if(isInWindowPoint(r, p)){
								context.write(new Text(r.toString()),new Text(p.toString()));
							}
						}
				}

			}catch(Exception ex){
				ex.printStackTrace();
				System.exit(1);
			}
		}
	}
}
