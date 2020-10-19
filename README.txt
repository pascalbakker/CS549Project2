Project 2 -  CS585 - Fall 2020

Team members:
	Pascal Bakker
	Mario Arduz

Project Division:
	Problem 1 and 3 Pascal Bakker
	Problem 2 and 4 Mario Arduz

Build

	mvn clean package

Run Hadoop Jobs

	hadoop jar target/project2-1.0-SNAPSHOT.jar [1-4]

	where [1-4] is the problem to execute

	For problem 4 you must pass two more arguements: radius and k clusters
	hadoop jar target/project2-1.0-SNAPSHOT.jar 4 10 3

The following is for if you get confused:

Problem 1 Information
	The output data will be in the following format
	Rectangle X coordinate, Rectangle Y coordinate, Rectangle Width, Rectangle Height, Point X, Point Y

Problem 2 Information
	/path/to/points_file.csv:
		is the file with the data set, located in hdfs, like /user/hadoop/data/points.csv
	/path/to/output:
		is the hdfs direction where outputs are stored, like /user/hadoop/output
	/path/to/centroids.txt:
		are the initial centroids in hdfs, like	/user/hadoop/data/centroids.txt

Problem 3 Information
	output is the following:
		id, flag, max elevation, min elevation.
	There will be 5 splits of about 193524 bytes each. This because there is 967120 byes in the json file


Problem 4 information
	/path/to/points_file.csv:
		is the file with the training set, located in hdfs, like /user/hadoop/data/points.csv (remember the condition of only having positive numbers)
	/path/to/output:
		is the hdfs direction where outputs are stored, like /user/hadoop/output
	Radius:
		is the radius around which neighbors are found
	K:
		is the necessary number of neighbors that does not make a point an outlier
