javac DataGenerator.java
java DataGenerator

hadoop dfs -mkdir ../data
hadoop dfs -copyFromLocal Transactions.txt ../data
hadoop dfs -copyFromLocal Customers.txt ../data
