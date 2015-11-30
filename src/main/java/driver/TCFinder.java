package driver;

import mapReduce.CoverageDensityConnectionReducer;
import geometry.*;
import mapReduce.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

public class TCFinder
{
    private static double distanceThreshold = 0.00005;
    private static int densityThreshold = 3;
    private static int timeInterval = 50;
    private static int lifetime = 5;
    private static int numSubPartitions = 1;

    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().
                setAppName("SparkTrajectoryCompanionFinder").
                setMaster("local[*]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile("hdfs://localhost:9000/data/Trajectories1.txt");

        // partition the entire data set into trajectory slots
        JavaPairRDD<Integer, Iterable<TCPoint>> slotsRDD =
            file.mapToPair(new TrajectorySlotMapper(timeInterval)).groupByKey().sortByKey();

        // partition each slot into sub partition
        JavaRDD<Tuple3<Integer, Integer, TCRegion>> subPartitionsRDD =
            slotsRDD.flatMap(new SubPartitionMapper(numSubPartitions, distanceThreshold));

        // find coverage density connection in each sub partition
        // merge coverage density connection per slot
        JavaPairRDD<Integer, List<Tuple2<Integer, Integer>>> densityConnectionRDD =
                subPartitionsRDD.mapToPair(new CoverageDensityConnectionMapper(densityThreshold))
                        .reduceByKey(new CoverageDensityConnectionReducer())
                        .sortByKey();

        // invert indexes base on the density connection found. such that
        // the key is the accompanied object pair; the value is the slot id
        JavaPairRDD<String, Integer> densityConnectionInvertedIndexRDD = densityConnectionRDD.
                flatMapToPair(new CoverageDensityConnectionInvertedIndexer()).distinct();
        JavaPairRDD<String, Iterable<Integer>> TCMapRDD
                = densityConnectionInvertedIndexRDD.groupByKey();

        // find continuous trajectory companions
        JavaPairRDD<String, Iterable<Integer>> resultRDD =
                TCMapRDD.filter(new TrajectoryCompanionFilter(lifetime));

        System.out.println(String.format(
                "%s result(s) found. Display first 10th results", resultRDD.count()));
        resultRDD.take(10).forEach((r)-> System.out.println(r.toString()) );

        //resultRDD.saveAsTextFile("hdfs://localhost:9000/data/Trajectories_output.txt");

        ctx.stop();
    }


}
