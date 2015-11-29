package driver;

import geometry.TCPoint;
import partition.PartitionMapToSlot;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TCFinder
{
    private static double distanceThreshold = 0.0001;
    private static int densityThreshold = 3;
    private static int timeInterval = 50;
    private static int lifetime = 3;
    private static int numSubPartitions = 3;

    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().
                setAppName("SparkTrajectoryCompanions").
                setMaster("local[2]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile("hdfs://localhost:9000/data/Trajectories1.txt");

        // partition the entire data set into trajectory slots,
        // key: slot id
        // value: list of points belong to the slot
        JavaPairRDD<Integer, Iterable<TCPoint>> slotsRDD =
        file.mapToPair(new PartitionMapToSlot(timeInterval)).groupByKey().cache();

        ctx.stop();
    }
}
