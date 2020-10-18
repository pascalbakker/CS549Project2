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

## To Run Problems 1 and 3

	rm -rf data/results;mvn package;hadoop jar target/project2-1.0-SNAPSHOT.jar [1,3]

	where [1,3] you type 1 or 3 for the job you want to do

## To Run Problems 2 and 4
	run the start.sh in each folder

	Results for hadoop jobs are stored in data/results
