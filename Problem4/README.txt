## To start Outlier.java

./outlier.sh /path/points_file.csv /path/to/output Radius K 

## Where
	/path/to/points_file.csv: 
		is the file with the training set, located in hdfs, like /user/hadoop/data/points.csv (remember the condition of only having positive numbers) 
	/path/to/output:
		is the hdfs direction where outputs are stored, like /user/hadoop/output 
	Radius:
		is the radius around which neighbors are found
	K:
		is the necessary number of neighbors that does not make a point an outlier