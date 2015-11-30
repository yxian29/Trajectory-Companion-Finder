package driver;

import geometry.TCPoint;
import geometry.TCRegion;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import partition.SubPartitionMapper;
import partition.TrajectorySlotMapper;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

public class TCFinder
{
    private static double distanceThreshold = 0.0001;
    private static int densityThreshold = 3;
    private static int timeInterval = 50;
    private static int lifetime = 3;
    private static int numSubPartitions = 1;

    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().
                setAppName("SparkTrajectoryCompanions").
                setMaster("local[1]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile("hdfs://localhost:9000/data/Trajectories1.txt");

        // Stage1: partition the entire data set into trajectory slots
        JavaPairRDD<Integer, Iterable<TCPoint>> slotsRDD =
            file.mapToPair(new TrajectorySlotMapper(timeInterval)).groupByKey().sortByKey();

        // Stage2: partition each slot into sub partition
        JavaRDD<Tuple3<Integer, Integer, TCRegion>> subPartitionsRDD =
            slotsRDD.flatMap(new SubPartitionMapper(numSubPartitions, distanceThreshold));



        ctx.stop();
    }
}
