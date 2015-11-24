package spark;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.List;

public class SparkTC 
{
    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().setAppName("SparkTrajectoryCompanions");
    	JavaSparkContext sc = new JavaSparkContext(sparkConf);
    	
    	sc.stop();
    }
}
