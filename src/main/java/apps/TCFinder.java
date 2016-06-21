package apps;
import common.data.TCPoint;
import common.data.TCPolyline;
import common.data.TCRegion;
import org.apache.spark.HashPartitioner;
import tc.*;
import common.cli.CliParserBase;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class TCFinder
{
    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.0001;   //eps
    private static int densityThreshold = 3;            //mu
    private static int timeInterval = 50;               //T
    private static int durationThreshold = 3;           //k
    private static int numSubPartitions = 2;            //n
    private static int sizeThreshold = 2;               //l
    private static boolean debugMode = false;

    public static void main( String[] args )
    {
        TCBatchCliParser parser = new TCBatchCliParser(args);
        parser.parse();

        if(parser.getCmd() == null)
            System.exit(0);

        initParams(parser);

    	SparkConf sparkConf = new SparkConf().
                setAppName("trajectory_companion_finder");

        // force to local mode if it is debug
        if(debugMode) sparkConf.setMaster("local[*]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile(inputFilePath, numSubPartitions);

        // partition the entire common.data set into trajectory slots
        // format: <slot_id, { pi, pj,... }>
        JavaPairRDD<Long, Iterable<TCPoint>> slotsRDD =
            file.mapToPair(new TrajectorySlotMapper(timeInterval))
                    .partitionBy(new HashPartitioner(numSubPartitions))
                    .groupByKey();

        // partition each slot into sub-partitions
        // format: <slot_id, TCRegion>
        JavaRDD<Tuple2<Long, TCRegion>> subPartitionsRDD =
                slotsRDD.flatMap(new KDTreeSubPartitionMapper(numSubPartitions)).cache();

        // get each point per partition
        // format: <(slotId, regionId), <objectId, point>>
        JavaPairRDD<String, Tuple2<Integer, TCPoint>> pointsRDD =
                subPartitionsRDD.flatMapToPair(new SubPartitionToPointsFlatMapper());

        // get all polylines per partition
        // format: <(slotId, regionId), {<objectId, polyline>}
        JavaPairRDD<String, Map<Integer, TCPolyline>> polylinesRDD =
                subPartitionsRDD.mapToPair(new SubPartitionToPolylinesMapper());

        // get density reachable per sub partition
        // format: <(slotId, regionId, objectId), {objectId}>
        JavaPairRDD<String, Iterable<Integer>> densityReachableRDD =
                pointsRDD.join(polylinesRDD)
                .flatMapToPair(new CoverageDensityReachableMapper(distanceThreshold))
                .groupByKey().filter(new CoverageDensityReachableFilter(densityThreshold));

        // remove objectId from key
        // format: <(slotId, regionId), {objectId}>
        JavaPairRDD<String, Iterable<Integer>> densityConnectionRDD
                = densityReachableRDD
                        .mapToPair(new SubPartitionRemoveObjectIDMapper());

        // merge density connection sub-partitions
        // format: <(slotId, regionId), {{objectId}}>
        JavaPairRDD<String, Iterable<Integer>> subpartMergeConnectionRDD =
                densityConnectionRDD
                .reduceByKey(new CoverageDensityConnectionReducer());

        // remove regionId from key
        // format: <slotId, {objectId}>
        JavaPairRDD<Integer, Iterable<Integer>> slotConnectionRDD =
        subpartMergeConnectionRDD
                .mapToPair(new SlotRemoveSubPartitionIDMapper())
                .reduceByKey(new CoverageDensityConnectionReducer());

        // obtain trajectory companion
        // format: <{objectId}, {slotId}>
        JavaPairRDD<String, Iterable<Integer>> companionRDD =
                slotConnectionRDD
                .flatMapToPair(new CoverageDensityConnectionSubsetMapper(sizeThreshold))
                .mapToPair(new CoverageDensityConnectionMapper())
                .groupByKey()
                .filter(new TrajectoryCompanionFilter(durationThreshold));

        if(debugMode)
            companionRDD.take(1);
        else
            companionRDD.saveAsTextFile(outputDir);

        ctx.stop();
    }

    private static void initParams(TCBatchCliParser parser)
    {
        String foundStr = CliParserBase.ANSI_GREEN + "param -%s is set. Use custom value: %s" + TCBatchCliParser.ANSI_RESET;
        String notFoundStr = CliParserBase.ANSI_RED + "param -%s not found. Use default value: %s" + TCBatchCliParser.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // input
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_INPUTFILE)) {
                inputFilePath = cmd.getOptionValue(TCBatchCliParser.OPT_STR_INPUTFILE);
            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

            // output
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(TCBatchCliParser.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(TCBatchCliParser.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(TCBatchCliParser.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_TIMEINTERVAL)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(TCBatchCliParser.OPT_STR_TIMEINTERVAL));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_TIMEINTERVAL, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_TIMEINTERVAL, timeInterval));
            }

            // life time
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_LIFETIME)) {
                durationThreshold = Integer.parseInt(cmd.getOptionValue(TCBatchCliParser.OPT_STR_LIFETIME));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_LIFETIME, durationThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_LIFETIME, durationThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(TCBatchCliParser.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_NUMPART, numSubPartitions));
            }

            // size threshold
            if (cmd.hasOption(TCBatchCliParser.OPT_STR_SIZETHRESHOLD)) {
                sizeThreshold = Integer.parseInt(cmd.getOptionValue(TCBatchCliParser.OPT_STR_SIZETHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCBatchCliParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            }
            else {
                System.out.println(String.format(notFoundStr,
                        TCBatchCliParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            }
        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }
}
