# Trajectory Companion Finder
---
This is a prototyping for analyzing a subset of [GeoLife GPS Trajectories Dataset](https://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/). Each tuple contains the information of latitude, longtitude and timestamp of an object. The goal is to discover trajectory companions of objects at a given continous trajectory slots.

# Thesis
[Streaming Data Algorithm Design for Big Trajectory Data Analysis](https://spectrum.library.concordia.ca/982628/)

## Prerequisite
* Setup Spark ([See instruction](http://spark.apache.org/docs/latest/spark-standalone.html))
* Setup Hadoop ([See instruction](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html))
(Optional, if the data is stored in Hadoop file system)
* Install Maven ([See instruction](https://maven.apache.org/install.html))
* Install Git

## Getting the source
To clone this repository to local, use the Git `clone` command.
```
$ git clone https://github.com/samsonxian/Trajectory-Companion-Finder.git
```

## Build
The project is managed by Maven. So simply execute maven `package` command at the same directory where `pom.xml` is located to generate jar file.
```
$ mvn package
```
The command line will print out various actions, and end with the following, indicating the build has succeed.
```
[INFO] [jar:jar {execution: default-jar}]
[INFO] Building jar: /home/<username>/TCFinder/target/TCFinder-0.0.1-SNAPSHOT.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESSFUL
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 23 seconds
[INFO] Finished at: Sun Feb 14 12:58:34 EST 2016
[INFO] Final Memory: 57M/337M
[INFO] ------------------------------------------------------------------------
```
## Prepare Input data
The subset of trajectory dataset is located at `/data` directory. However, the dataset raw data are in various formats. You may use the scripts provided under `/scripts` to covert the raw data into csv format. (such that
``
objectId, lat, long, timestamp
``)

## Running application
Now you are ready to run the application locally or deploy it to clusters. Specifically, to run the application on AWS EC2, please following this [guideline](spark.apache.org/docs/latest/ec2-scripts.html).
To display a list of available arguments, enter
```
$ $SPARK_HOME/bin/spark-submit --class driver.TCFinder /TCFinder-0.0.1-SNAPSHOT.jar -help
```
arguments:
```
-e <arg>   distance threshold (optional)
-h         show help
-help      show help
-i <arg>   input file (required).
-k <arg>   life time (optional)
-n <arg>   number of sub-partitions (optional)
-o <arg>   output directory (requied)
-T <arg>   time interval of a trajectory slot (optional)
-u <arg>   density threshold (optional)
```
note: if optional parameter is not specified, default value will be used.

note: use wild card to define multiple input files. e.g. hdfs://localhost:9000/dataset/*.txt

#### local mode
To run the program locally, simply type the following into command line
```
$ $SPARK_HOME/bin/spark-submit --master local --class driver.TCFinder /TCFinder-0.0.1-SNAPSHOT.jar -i hdfs://localhost:9000/dataset1/Trajectory_1.txt -o hdfs://localhost:9000/output
```
note:
- local: use one worker thread (i.e. no parallisom at all)
- local[*]: use as many worker threads as logical cores on your machine.
- local[k]: use k worker threads

#### cluster mode
In addition to running on local mode, Spark can be run on the Mesos or YARN cluster managers, as well as standalone deploy mode. Spark also comes with provisioning script allowing quick and easy deployment to AWS EC2 (see [this](https://spark.apache.org/docs/latest/ec2-scripts.html)). Alternatively, you can follow [this](https://spark.apache.org/docs/latest/spark-standalone.html) to configure for any other clusters manually.

After setting up the cluster, you can run the program on the master node.
```
$ $SPARK_HOME/bin/spark-submit --master spark://ec2-54-175-128-116.compute-1.amazonaws.com:7077 --class driver.TCFinder /TCFinder-0.0.1-SNAPSHOT.jar -i s3n://ec2clusterdata/TCFinder/Trajectories1.txt -o s3n://ec2clusterdata/TCFinder/output
```
another example with arguments
```
$ $SPARK_HOME/bin/spark-submit --master spark://ec2-54-175-128-116.compute-1.amazonaws.com:7077 --class driver.TCFinder /TCFinder-0.0.1-SNAPSHOT.jar -i s3n://ec2clusterdata/TCFinder/Trajectories1.txt -o s3n://ec2clusterdata/TCFinder/output -n 4
```
## Expected Output
The outputs are trajectory companions in the format
``
((objectId1, objectId2), [{trajectory slot id}]
``

e.g. if lifetime k = 5, the output should look like,
```
((58,152),[3, 5, 1, 2, 4])
((118,151),[4, 9, 5, 7, 10, 3, 1, 8, 2, 6])
((3,118),[9, 10, 7, 2, 4, 8, 6, 1, 3, 5])
((65,130),[7, 4, 6, 3, 5, 2])
((71,177),[3, 1, 5, 2, 4])
((27,82),[6, 2, 3, 0, 1, 4, 5])
((32,149),[8, 1, 7, 3, 4, 2, 6, 5])
((33,111),[7, 8, 2, 9, 1, 3, 6, 4, 5])

```
