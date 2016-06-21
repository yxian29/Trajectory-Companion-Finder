package apps;

import common.cli.CliParserBase;
import common.cli.Config;
import common.cli.PropertyFileParser;
import common.data.Crowd;
import common.data.DBSCANCluster;
import common.data.UserData;
import common.data.TCPoint;
import gp.*;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import sgp.SGPCliParser;
import stc.BatchCountFunc;
import stc.InputDStreamValueMapper;

import java.util.*;

public class StreamingGPFinder {

    private static String outputDir = "";
    private static double distanceThreshold = 0.01;     // eps
    private static int densityThreshold = 2;            // mu
    private static int timeInterval = 60;               // delta t
    private static int lifetimeThreshold = 100;         // kc
    private static int clusterNumThreshold = 3;         // kp
    private static int participatorNumThreshold = 2;    // mp
    private static int numSubPartitions = 2;
    private static boolean debugMode = false;

    public static void main(String[] args) throws Exception {

        // setup the cli parser
        SGPCliParser parser = new SGPCliParser(args);
        parser.parse();

        if(parser.getCmd() == null) {
            System.exit(1);
        }

        final UserData data = new UserData();
        initParams(parser, data);

        // setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", propertyParser.getProperty(Config.KAFKA_BROKERS));

        SparkConf sparkConf = new SparkConf()
                .setAppName("StreamingGPFinder");
        if(debugMode) sparkConf.setMaster("local[*]");

        int batchInterval = Integer.parseInt(propertyParser.getProperty(Config.SPARK_BATCH_INTERVAL));
        final JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
        ssc.checkpoint(propertyParser.getProperty(Config.SPARK_CHECKPOINT_DIR));
        Set<String> topics = new HashSet(Arrays.asList(
                propertyParser.getProperty(Config.KAFKA_TOPICS).split(",")));

        // create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> inputDStream =
                KafkaUtils.createDirectStream(
                        ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class,
                        kafkaParams, topics);

        JavaPairDStream windowedInputRDD = inputDStream.window(Durations.seconds(batchInterval));
        JavaDStream<String> lines = windowedInputRDD.map(new InputDStreamValueMapper());
        lines.foreach(new BatchCountFunc());

        // find snapshot per timestamp and data partition
        // format: <timestamp, {point}>
        JavaPairDStream<Integer, Iterable<TCPoint>> snapshotDStream =
                lines.mapToPair(new SnapshotMapper()).groupByKey();

        // find clusters - find clusters (DBSCAN) in each sub-partition
        // format: <timestamp, {cluster}>
        JavaPairDStream<Integer, DBSCANCluster> clusterDStream =
                snapshotDStream.flatMapToPair(new DBSCANClusterMapper(distanceThreshold, densityThreshold));

        clusterDStream.foreachRDD(new Function2<JavaPairRDD<Integer, DBSCANCluster>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, DBSCANCluster> clusterRDD, Time time) throws Exception {

                // group clusters by timestamp to form a crowd, given each crowd
                // an unique id
                // format: <<timestamp, crowd>, crowdId>
                JavaPairRDD<Tuple2<Integer, Crowd>, Long> crowdRDD =
                        GPQuery.getCrowdRDDByRangePartitionJoin(clusterRDD, data);

                // find participator in a given crowd
                // format: <crowdId, {participator}>
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
                    gatheringRDD.saveAsTextFile(outputDir);

                return null;
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }

    private static void initParams(SGPCliParser parser, UserData data)
    {
        String foundStr = CliParserBase.ANSI_GREEN + "param -%s is set. Use custom value: %s" + CliParserBase.ANSI_RESET;
        String notFoundStr = CliParserBase.ANSI_RED + "param -%s not found. Use default value: %s" + CliParserBase.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // output
            if (cmd.hasOption(SGPCliParser.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(SGPCliParser.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(SGPCliParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(SGPCliParser.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(SGPCliParser.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(SGPCliParser.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(SGPCliParser.OPT_STR_TIMETHRESHOLD)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_TIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_TIMETHRESHOLD, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_TIMETHRESHOLD, timeInterval));
            }

            // life time
            if (cmd.hasOption(SGPCliParser.OPT_STR_LIFETIMETHRESHOLD)) {
                lifetimeThreshold = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_LIFETIMETHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_LIFETIMETHRESHOLD, lifetimeThreshold));
            }

            // cluster number threshold
            if (cmd.hasOption(SGPCliParser.OPT_STR_CLUSTERNUMTHRESHOLD)) {
                clusterNumThreshold = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_CLUSTERNUMTHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_CLUSTERNUMTHRESHOLD, clusterNumThreshold));
            }

            // participator number threshold
            if (cmd.hasOption(SGPCliParser.OPT_STR_PARTICIPATORTHRESHOLD)) {
                participatorNumThreshold = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_PARTICIPATORTHRESHOLD));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_PARTICIPATORTHRESHOLD, participatorNumThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(SGPCliParser.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(SGPCliParser.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        SGPCliParser.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGPCliParser.OPT_STR_NUMPART, numSubPartitions));
            }

            // default user data
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
