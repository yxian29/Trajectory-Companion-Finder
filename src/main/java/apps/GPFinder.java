package apps;

import common.Utils.CmdParser;
import GatheringPattern.GPCmdParser;
import common.geometry.TCPoint;
import common.geometry.TCRegion;
import GatheringPattern.DBSCANClusterMapper;
import TrajectoryCompanion.KDTreeSubPartitionMapper;
import TrajectoryCompanion.TrajectorySlotMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class GPFinder {

    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.005;    // eps
    private static int densityThreshold = 3;            // mu
    private static int timeInterval = 60;               // delta t
    private static int lifetimeThreshold = 100;         // kc
    private static int clusterNumThreshold = 3;         // kp
    private static int participatorNumThreshold = 3;    // mp
    private static int numSubPartitions = 2;
    private static boolean debugMode = false;

    public static void main( String[] args ) {

        GPCmdParser parser = new GPCmdParser(args);
        parser.parse();

        if(parser.getCmd() == null)
            System.exit(0);

        initParams(parser);

        SparkConf sparkConf = new SparkConf().
                setAppName("GPFinder");

        // force to local mode if it is debug
        if(debugMode) sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile(inputFilePath);

        // TODO: data partition
        JavaPairRDD<Integer, Iterable<TCPoint>> slotsRDD =
                file.mapToPair(new TrajectorySlotMapper(timeInterval)).groupByKey();

        JavaRDD<Tuple2<Integer, TCRegion>> subPartitionsRDD =
                slotsRDD.flatMap(new KDTreeSubPartitionMapper(numSubPartitions));

        // TODO: find
        JavaPairRDD<String, Cluster> testRDD =
                subPartitionsRDD.flatMapToPair(new DBSCANClusterMapper(distanceThreshold,
                        densityThreshold));

        // TODO: find same objects

        // TODO: merge clusters

        // TODO: discover gatherings

        ctx.stop();
    }

    private static void initParams(GPCmdParser parser)
    {
        String foundStr = CmdParser.ANSI_GREEN + "param -%s is set. Use custom value: %s" + CmdParser.ANSI_RESET;
        String notFoundStr = CmdParser.ANSI_RED + "param -%s not found. Use default value: %s" + CmdParser.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // input
            if (cmd.hasOption(GPCmdParser.OPT_STR_INPUTFILE)) {
                inputFilePath = cmd.getOptionValue(GPCmdParser.OPT_STR_INPUTFILE);
            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

            // output
            if (cmd.hasOption(GPCmdParser.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(GPCmdParser.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(GPCmdParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(GPCmdParser.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(GPCmdParser.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(GPCmdParser.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(GPCmdParser.OPT_STR_TIMETHRESHOLD)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_TIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_TIMETHRESHOLD, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_TIMETHRESHOLD, timeInterval));
            }

            // life time
            if (cmd.hasOption(GPCmdParser.OPT_STR_LIFETIMETHRESHOLD)) {
                lifetimeThreshold = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_LIFETIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            }

            // cluster number threshold
            if (cmd.hasOption(GPCmdParser.OPT_STR_CLUSTERNUMTHRESHOLD)) {
                clusterNumThreshold = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_CLUSTERNUMTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            }

            // participator number threshold
            if (cmd.hasOption(GPCmdParser.OPT_STR_PARTICIPATORTHRESHOLD)) {
                participatorNumThreshold = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_PARTICIPATORTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(GPCmdParser.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(GPCmdParser.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        GPCmdParser.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPCmdParser.OPT_STR_NUMPART, numSubPartitions));
            }
        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }
}
