package edu.wpi.project2;
import java.util.Random;

/**
   USED IN PROBLEM 1
*/
public class RectangleData extends PointData {
   double x;
   double y;
   double height;
   double width;

   // Generate random rectangle x in [0,10000] y in [0,10000] height in [1,20] width in [1,5]
   RectangleData () {
      Random random = new Random();
      this.x = random.nextDouble()*10001.0;
      this.y = random.nextDouble()*10001.0;
      this.height = random.nextDouble()*20.0+1;
      this.width = random.nextDouble()*5.0+1;
   }

   RectangleData (double x,double y, double height, double width) {
      super(x,y);
      this.x = x;
      this.y = y;
      this.height = height;
      this.width = width;
   }

   public String toString(){
         return this.x+","+this.y+","+this.height+","+this.width;
   }
 }
