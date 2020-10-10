# Project 2 -  CS585 - Fall 2020
## Homework 2 for big data.

Team members:
	Pascal Bakker
	Mario Arduz

Project Division:
	Problem 1: Pascal
	Problem 2: Mario
	Problem 3: Pascal
	Problem 4: Mario

### Problem 1
	Generate Data(Note: currently need to move the csv files to the data folder)
	run start.sh in DataGenerator/Problem1/start.sh

### Build

	mvn clean package

### Run Hadoop Jobs


	rm -rf data/results;mvn package;hadoop jar target/project2-1.0-SNAPSHOT.jar

	Results for hadoop jobs are stored in data/results
