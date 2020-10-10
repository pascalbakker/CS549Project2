package edu.wpi.project2;
import java.util.Random;

/**
   USED IN PROBLEM 1

*/
public class PointData {
   double x;
   double y;

   // Generate random point between x in [0,10000] y in [0,10000]
   PointData () {
      Random random = new Random();
      this.x = random.nextDouble()*10001.0;
      this.y = random.nextDouble()*10001.0;
   }

   PointData (double x, double y) {
      this.x = x;
      this.y = y;
   }
   public String toString(){
         return this.x +","+this.y;
   }
 }
