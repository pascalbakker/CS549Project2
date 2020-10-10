package edu.wpi.project1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Query2{

	// Customer id,name,age,gender,country,salary
	// Transactions transid, customer_id, total, num_items, desc

	// Mapper for customers
	public static class CustomMapper extends Mapper<Object, Text, Text, Text>
	{
		// combiner done.
		private static HashMap<Integer, String> customerHashpMap = new HashMap<>();

		// Retrieve customer data
		public void setup(Context context) throws IOException, InterruptedException {
			try{
					URI[] customerFile = context.getCacheFiles();
					if (customerFile != null && customerFile.length > 0) {
						for (URI file : customerFile){
							// Read Customers
							FileSystem f = FileSystem.get(context.getConfiguration());
							Path p = new Path(file.toString());
							BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(f.open(p)));
							String value;
							String[] customerData;
							while ((value = bufferedReader.readLine()) != null) {
								customerData = value.split(",");
								customerHashpMap.put(Integer.parseInt(customerData[0]), customerData[1]);
							}
						}
					}
			} catch (IOException ex){
						System.exit(1);
				}
		}


		  // Parse customer input
		  // Get Customer_id, Name
	      public void map(Object key,
							Text value,
							Context context) throws IOException, InterruptedException {
	        String[] transData = value.toString().split(",");
			//System.out.println(transData[1].toString());
			String customerName = customerHashpMap.get(Integer.parseInt(transData[1]));
			Text mapkey = new Text(transData[1].toString()); // customer id
			System.out.println(customerName);
			String transValue = transData[2];
			Text mapvalue = new Text(customerName+","+transValue);
			context.write(mapkey, mapvalue);
	      }

	}


	// input: key customer_id, values "customerName,transValue"
	// output: key customer_id, values "customerName,totalCount,totalSum"
	public static class CustomCombiner extends Reducer <Text,Text,Text,Text>{
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				float sum = 0;
				int count = 0;
				String name = "";
				for (Text t: values){
					//System.out.println(t.toString());
					String[] data = t.toString().split(",");
					name = data[0];
					sum+= Double.parseDouble(data[1]);
					count++;
				}
				Text combinerValue = new Text(name+","+count+","+sum);
				context.write(key, combinerValue);
			}
	}

	// input: key customer_id, values: ["customerName,count,sum"]
	// output: key customer_id, values: ["customerName,count,sum"]
	public static class CustomReducer extends Reducer<Text,Text,Text,Text>{
		// values: { customer_id - [customer_name, amount1, amount2 ...] }
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 Float sum = 0F;
                int count = 0;
                String name = "";
                for (Text t : values) {
					//System.out.println(val.toString());
                    String[] data = t.toString().split(",");
                    sum += Float.parseFloat(data[2]);
                    name = data[0];
                    count += Integer.parseInt(data[1]);
                }
                context.write(key, new Text(name + "," + count + "," + sum));
		}
	}
}
