package driver;

import Utils.Cli;
import mapReduce.CoverageDensityConnectionReducer;
import geometry.*;
import mapReduce.*;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

public class TCFinder
{
    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.00005;
    private static int densityThreshold = 3;
    private static int timeInterval = 50;
    private static int lifetime = 5;
    private static int numSubPartitions = 1;

    public static void main( String[] args )
    {
        Cli parser = new Cli(args);
        parser.parse();

        if(parser.getCmd() == null)
            System.exit(0);

        initParams(parser);

    	SparkConf sparkConf = new SparkConf().
                setAppName("TrajectoryCompanionFinder");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile(inputFilePath);

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

        if(outputDir != "") {
            System.out.println(String.format("Saving result to %s", outputDir));
            resultRDD.saveAsTextFile(outputDir);
        }

        ctx.stop();
    }

    private static void initParams(Cli parser)
    {
        String notFoundStr = "param -%s not found. Use default value: %s";
        CommandLine cmd = parser.getCmd();

        try {

            // input
            if (cmd.hasOption(Cli.OPT_STR_INPUTFILE)) {
                inputFilePath = cmd.getOptionValue(Cli.OPT_STR_INPUTFILE);
            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

            // distance threshold
            if (cmd.hasOption(Cli.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(Cli.OPT_STR_DISTTHRESHOLD));
            } else {
                System.out.println(String.format(notFoundStr,
                        Cli.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(Cli.OPT_STR_DISTTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(Cli.OPT_STR_DENTHRESHOLD));
            } else {
                System.out.println(String.format(notFoundStr,
                        Cli.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(Cli.OPT_STR_TIMEINTERVAL)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(Cli.OPT_STR_TIMEINTERVAL));
            } else {
                System.out.println(String.format(notFoundStr,
                        Cli.OPT_STR_TIMEINTERVAL, timeInterval));
            }

            // life time
            if (cmd.hasOption(Cli.OPT_STR_LIFETIME)) {
                lifetime = Integer.parseInt(cmd.getOptionValue(Cli.OPT_STR_LIFETIME));
            } else {
                System.out.println(String.format(notFoundStr,
                        Cli.OPT_STR_LIFETIME, lifetime));
            }

            // number of  sub-partitions
            if (cmd.hasOption(Cli.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(Cli.OPT_STR_NUMPART));
            } else {
                System.out.println(String.format(notFoundStr,
                        Cli.OPT_STR_NUMPART, numSubPartitions));
            }
        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }
}
