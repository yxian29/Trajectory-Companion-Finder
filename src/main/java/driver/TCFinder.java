package driver;

import org.apache.spark.api.java.function.*;
import partition.TrajectorySlotPartitioner;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.util.Iterator;

public class TCFinder
{
    private static int timeInterval = 50;

    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().
                setAppName("SparkTrajectoryCompanions").
                setMaster("local[2]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile("hdfs://localhost:9000/data/Trajectories1.txt");

        // transform each line to a key-value pair. timestamp as key
        JavaPairRDD<Integer, Tuple3<Integer, Double, Double>> map = file.mapToPair(
                new PairFunction<String, Integer, Tuple3<Integer, Double, Double>>()
        {
            public Tuple2<Integer, Tuple3<Integer, Double, Double>> call(String s) {
                String[] split = s.split(",");
                Integer objectId = Integer.parseInt(split[0]);
                Double x = Double.parseDouble(split[1]);
                Double y = Double.parseDouble(split[2]);
                Integer timestamp = Integer.parseInt(split[3]);

                return new Tuple2<Integer, Tuple3<Integer, Double, Double>>(timestamp,
                        new Tuple3<Integer, Double, Double>(objectId, x, y));
            }
        });

        JavaPairRDD<Integer, Tuple3<Integer, Double, Double>> partitions =
                map.partitionBy(new TrajectorySlotPartitioner(4));

        partitions.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Tuple3<Integer, Double, Double>>>>() {
            public void call(Iterator<Tuple2<Integer, Tuple3<Integer, Double, Double>>> tuple2Iterator) throws Exception {
            }
        });

        ctx.stop();
    }
}
