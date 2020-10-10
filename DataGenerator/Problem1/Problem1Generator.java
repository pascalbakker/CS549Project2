import java.util.Random;
import java.awt.Point;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class Problem1Generator {

   public static void main(String[] args) throws IOException  {
         generateData();
    }

   public static void generateData() throws IOException{
      // write point data
      FileWriter myWriter = new FileWriter("PointData.csv");
      for (int i = 1; i<=50000; i++) {
         PointData p = new PointData();
         myWriter.write(p.toString()+"\n");
      }
      myWriter.close();
      // write rectangel data
      myWriter = new FileWriter("RectangleData.csv");
      for (int i = 1; i<=10000; i++) {
         RectangleData r = new RectangleData();
         myWriter.write(r.toString()+"\n");
      }
      myWriter.close();
   }


   }
class PointData {
   double x;
   double y;

   PointData () {
      Random random = new Random();
      this.x = random.nextDouble()*10001.0;
      this.y = random.nextDouble()*10001.0;
   }
   public String toString(){
         return this.x +","+this.y;
   }
 }

 class RectangleData {
   PointData p;
   double height;
   double width;
   RectangleData () {
      Random random = new Random();
      this.p = new PointData();
      this.height = random.nextDouble()*20.0+1;
      this.width = random.nextDouble()*5.0+1;
   }

   public String toString(){
         return p.toString()+","+height+","+width;
   }
 }
