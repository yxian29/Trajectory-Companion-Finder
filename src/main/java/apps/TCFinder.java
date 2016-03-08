package apps;

import TrajectoryCompanion.*;
import common.Utils.CmdParser;
import common.geometry.*;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TCFinder
{
    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.00005;
    private static int densityThreshold = 3;
    private static int timeInterval = 50;
    private static int durationThreshold = 3;
    private static int numSubPartitions = 2;
    private static int sizeThreshold = 3;
    private static boolean debugMode = false;

    public static void main( String[] args )
    {
        TCCmdParser parser = new TCCmdParser(args);
        parser.parse();

        if(parser.getCmd() == null)
            System.exit(0);

        initParams(parser);

    	SparkConf sparkConf = new SparkConf().
                setAppName("TrajectoryCompanionFinder");

        // force to local mode if it is debug
        if(debugMode) sparkConf.setMaster("local[*]");

    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile(inputFilePath, numSubPartitions);

        // partition the entire common.data set into trajectory slots
        // format: <slot_id, { pi, pj,... }>
        JavaPairRDD<Integer, Iterable<TCPoint>> slotsRDD =
            file.mapToPair(new TrajectorySlotMapper(timeInterval)).groupByKey();

        // partition each slot into sub-partitions
        // format: <slot_id, TCRegion>
        JavaRDD<Tuple2<Integer, TCRegion>> subPartitionsRDD =
                slotsRDD.flatMap(new KDTreeSubPartitionMapper(numSubPartitions));

        // find objects that are coverage density reachable from each
        // format: <(sid, rid, oid), {Oi,Oj,...}>
        JavaPairRDD<String, Iterable<Integer>> densityReachableRDD =
                subPartitionsRDD.flatMapToPair(new CoverageDensityReachableMapper(distanceThreshold))
                .groupByKey()
                .filter(new CoverageDensityReachableFilter(densityThreshold));

        // find coverage density connection in each sub-partition
        // format: <(sid, rid), {Oi, Oj, ...}>
        JavaPairRDD<String, Iterable<Integer>> densityConnectionRDD =
                densityReachableRDD.flatMapToPair(new CoverageDensityConnectionMapper())
                .reduceByKey(new CoverageDensityConnectionReducer())
                .filter(new CoverageDensityConnectionFilter(sizeThreshold));

        // invert indexes base on the density connection found. such that
        // the key is the accompanied objects; the value is the slot id
        JavaPairRDD<String, Iterable<Integer>> densityConnectionInvertedIndexRDD = densityConnectionRDD.
                mapToPair(new CoverageDensityConnectionInvertedIndexer())
                .distinct()
                .groupByKey();

        // find continuous trajectory companions
        JavaPairRDD<String, Iterable<Integer>> resultRDD =
                densityConnectionInvertedIndexRDD.filter(new TrajectoryCompanionFilter(durationThreshold));

        System.out.println(String.format("Saving result to %s", outputDir));
        //resultRDD.saveAsTextFile(outputDir);
        resultRDD.take(1);

        ctx.stop();
    }

    private static void initParams(TCCmdParser parser)
    {
        String foundStr = CmdParser.ANSI_GREEN + "param -%s is set. Use custom value: %s" + TCCmdParser.ANSI_RESET;
        String notFoundStr = CmdParser.ANSI_RED + "param -%s not found. Use default value: %s" + TCCmdParser.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // input
            if (cmd.hasOption(TCCmdParser.OPT_STR_INPUTFILE)) {
                inputFilePath = cmd.getOptionValue(TCCmdParser.OPT_STR_INPUTFILE);
            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

            // output
            if (cmd.hasOption(TCCmdParser.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(TCCmdParser.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(TCCmdParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(TCCmdParser.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(TCCmdParser.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(TCCmdParser.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(TCCmdParser.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(TCCmdParser.OPT_STR_TIMEINTERVAL)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(TCCmdParser.OPT_STR_TIMEINTERVAL));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_TIMEINTERVAL, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_TIMEINTERVAL, timeInterval));
            }

            // life time
            if (cmd.hasOption(TCCmdParser.OPT_STR_LIFETIME)) {
                durationThreshold = Integer.parseInt(cmd.getOptionValue(TCCmdParser.OPT_STR_LIFETIME));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_LIFETIME, durationThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_LIFETIME, durationThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(TCCmdParser.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(TCCmdParser.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_NUMPART, numSubPartitions));
            }

            // size threshold
            if (cmd.hasOption(TCCmdParser.OPT_STR_SIZETHRESHOLD)) {
                sizeThreshold = Integer.parseInt(cmd.getOptionValue(TCCmdParser.OPT_STR_SIZETHRESHOLD));
                System.out.println(String.format(foundStr,
                        TCCmdParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            }
            else {
                System.out.println(String.format(notFoundStr,
                        TCCmdParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            }
        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }
}
