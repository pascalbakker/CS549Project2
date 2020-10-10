javac Problem1Generator.java
java Problem1Generator

hadoop dfs -mkdir ../data
hadoop dfs -copyFromLocal PointData.csv ..
hadoop dfs -copyFromLocal RectangleData.csv ..
