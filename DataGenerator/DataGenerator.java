import java.util.Random;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class DataGenerator {
   public static void main(String[] args) throws IOException  {
      FileWriter myWriter = new FileWriter("Customers.txt");
      for (int i = 1; i<=50000; i++) {      
         Customers person = new Customers(i);
         String text = person.Id +","+person.Name+","+person.Age+","+
            person.Gender+","+person.CountryCode+","+person.Salary+"\n";
            myWriter.write(text);
      }
      myWriter.close();
      
      myWriter = new FileWriter("Transactions.txt");
      for (int i = 1; i<=5000000; i++) {      
         Transactions transaction = new Transactions(i);
         String text = transaction.TransID +","+transaction.CustID+","+transaction.TransTotal+","+
         transaction.TransNumItems+","+transaction.TransDesc+"\n";
            myWriter.write(text);
      }
      myWriter.close();
   }
 }
 
class Customers {
   int Id;
   String Name;
   int Age;
   String Gender;
   int CountryCode;
   int Salary;
   
   Customers (int i) {
      Random random = new Random();
      Id = i;
      Name = Name();
      Age = random.nextInt(60) + 10;
      if(random.nextInt(2) == 1) { Gender = "female";} else { Gender = "male";}
      CountryCode = random.nextInt(10) + 1;
      Salary = random.nextInt(9900) + 100;
   }
   
   static String Name() {
      int leftLimit = 97; // letter 'a'
      int rightLimit = 122; // letter 'z'
      int minl = 10;
      int maxl = 20;
      Random random = new Random();
      int stringLength = random.nextInt(maxl-minl) + minl;
      
      String word = "";
      
      for (int i = 0; i < stringLength; i++) {
         int randomLimitedInt = random.nextInt(rightLimit-leftLimit) + leftLimit;
          word += (char) randomLimitedInt;
     }

     return word;
  }

 }

 class Transactions {
   int TransID;
   int CustID;
   int TransTotal;
   int TransNumItems;
   String TransDesc;
   
   Transactions (int i) {
      Random random = new Random();
      TransID = i;
      CustID = random.nextInt(50000) + 1;
      TransTotal = random.nextInt(990) + 10;
      TransNumItems = random.nextInt(10) + 1;
      TransDesc = TransDesc();
   }
   
   static String TransDesc() {
      int leftLimit = 97; // letter 'a'
      int rightLimit = 122; // letter 'z'
      int minl = 10;
      int maxl = 20;
      Random random = new Random();
      int stringLength = random.nextInt(maxl-minl) + minl;
      
      String word = "";
      
      for (int i = 0; i < stringLength; i++) {
         int randomLimitedInt = random.nextInt(rightLimit-leftLimit) + leftLimit;
          word += (char) randomLimitedInt;
     }

     return word;
  }

 }
