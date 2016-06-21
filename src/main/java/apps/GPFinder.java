package apps;

import common.data.TCPoint;
import common.data.UserData;
import common.data.Crowd;
import common.data.DBSCANCluster;
import gp.*;
import common.cli.CliParserBase;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class GPFinder {

    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.0001;   // eps
    private static int densityThreshold = 3;            // mu
    private static int timeInterval = 60;               // delta t
    private static int lifetimeThreshold = 70;          // kc
    private static int clusterNumThreshold = 3;         // kp
    private static int participatorNumThreshold = 2;    // mp
    private static int numSubPartitions = 2;
    private static boolean debugMode = false;

    public static void main(String[] args) {

        GPBatchCliParser parser = new GPBatchCliParser(args);
        parser.parse();
        if (parser.getCmd() == null)
            System.exit(0);

        UserData data = new UserData();
        initParams(parser, data);

        SparkConf sparkConf = new SparkConf().
                setAppName("GPFinder");

        // force to local mode if it is debug
        if (debugMode) sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = ctx.textFile(inputFilePath);

        // find snapshot per timestamp and data partition
        // format: <timestamp, {point}>
        JavaPairRDD<Integer, Iterable<TCPoint>> snapshotRDD =
                GPQuery.getSnapshotRDD(file, data);

        // find clusters - find clusters (DBSCAN) in each sub-partition
        // format: <timestamp, {cluster}>
        JavaPairRDD<Integer, DBSCANCluster> clusterRDD =
                GPQuery.getClusterRDD(snapshotRDD, data);

        // group clusters by timestamp to form a crowd, given each crowd
        // an unique id
        // format: <<timestamp, crowd>, crowdId>
        JavaPairRDD<Tuple2<Integer, Crowd>, Long> crowdRDD =
                GPQuery.getCrowdRDDByRangePartitionJoin(clusterRDD, data);

//        // find participator in a given crowd
//        // format: <crowdId, {participator}>
        JavaPairRDD<Long, Iterable<Integer>> participatorRDD =
                GPQuery.getParticipatorRDD(crowdRDD, data);

        // convert crowd into the same format as participator
        // format: <crowdId, {(objectId, timestamp)}>
        JavaPairRDD<Long, Iterable<Tuple2<Integer, Integer>>> crowdToObjectTimestampRDD =
                crowdRDD.flatMapToPair(new CrowdToObjectTimestampPairMapper());

        // discover gatherings
        // format: <crowdId, {(timestamp, {objectId})}>
        JavaPairRDD<Long, Iterable<Tuple2<Integer, Iterable<Integer>>>> gatheringRDD =
                GPQuery.getGatheringRDD(crowdToObjectTimestampRDD, participatorRDD,
                data);

        if(outputDir.isEmpty())
            gatheringRDD.take(1);
        else
            gatheringRDD.saveAsHadoopFile(outputDir,
                    String.class, String.class, TextOutputFormat.class);

        ctx.stop();
    }

    private static void initParams(GPBatchCliParser parser, UserData data)
    {
        String foundStr = CliParserBase.ANSI_GREEN + "param -%s is set. Use custom value: %s" + CliParserBase.ANSI_RESET;
        String notFoundStr = CliParserBase.ANSI_RED + "param -%s not found. Use default value: %s" + CliParserBase.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // input
            if (cmd.hasOption(GPConstants.OPT_STR_INPUTFILE)) {
                inputFilePath = cmd.getOptionValue(GPConstants.OPT_STR_INPUTFILE);

            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

            // output
            if (cmd.hasOption(GPConstants.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(GPConstants.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(GPBatchCliParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(GPConstants.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(GPConstants.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(GPConstants.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(GPConstants.OPT_STR_TIMETHRESHOLD)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_TIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_TIMETHRESHOLD, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_TIMETHRESHOLD, timeInterval));
            }

            // life time
            if (cmd.hasOption(GPConstants.OPT_STR_LIFETIMETHRESHOLD)) {
                lifetimeThreshold = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_LIFETIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            }

            // cluster number threshold
            if (cmd.hasOption(GPConstants.OPT_STR_CLUSTERNUMTHRESHOLD)) {
                clusterNumThreshold = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_CLUSTERNUMTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            }

            // participator number threshold
            if (cmd.hasOption(GPConstants.OPT_STR_PARTICIPATORTHRESHOLD)) {
                participatorNumThreshold = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_PARTICIPATORTHRESHOLD));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(GPConstants.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(GPConstants.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        GPConstants.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        GPConstants.OPT_STR_NUMPART, numSubPartitions));
            }

            // default user data
            data.add(GPConstants.OPT_STR_INPUTFILE, inputFilePath);
            data.add(GPConstants.OPT_STR_OUTPUTDIR, outputDir);
            data.add(GPBatchCliParser.OPT_STR_DEBUG, debugMode);
            data.add(GPConstants.OPT_STR_DISTTHRESHOLD, distanceThreshold);
            data.add(GPConstants.OPT_STR_DENTHRESHOLD, densityThreshold);
            data.add(GPConstants.OPT_STR_TIMETHRESHOLD, timeInterval);
            data.add(GPConstants.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold);
            data.add(GPConstants.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold);
            data.add(GPConstants.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold);
            data.add(GPConstants.OPT_STR_NUMPART, numSubPartitions);
        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }
}
