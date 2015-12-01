# Trajectory Companion Finder
---
This is a prototyping for analyzing a subset of [GeoLife GPS Trajectories Dataset](https://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/). Each tuple contains the information of latitude, longtitude and timestamp of an object. The goal is to discover trajectory companions of objects at a given continous trajectory slots.

## Prerequisite
* Setup Spark ([See instruction](http://spark.apache.org/docs/latest/spark-standalone.html))
* Setup Hadoop ([See instruction](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html))
* Install Maven ([See instruction](https://maven.apache.org/install.html))

## Prepare Input data
The subset of trajectory dataset is located at `/data` directory. You may use the ParseFile.pl to covert the raw data into csv format. (such that
``
objectId, lat, long, timestamp
``)

``
ParseFile.pl [inputFile] [outputFile]
``

e.g.
```
$ Perl ParseFile.pl .\data\Trajectories.txt .\data\Trajectories1.csv
```

## Build
The project is managed by Maven. So simply execute maven package command at the same directory where `pom.xml` is located to generate jar file.
```
$ mvn package
```

## Run
```
$ spark-submit --class driver.TCFinder --master local[2] TCFinder-0.0.1-SNAPSHOT.jar
```
## Expected Output
The outputs are trajectory companions in the format
``
((objectId1, objectId2), [{trajectory slot id}]
``

e.g. if lifetime k = 5, the output should like,
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

