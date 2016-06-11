package apps;

import common.cli.CliParserBase;
import common.cli.Config;
import common.cli.PropertyFileParser;
import common.geometry.*;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import stc.BatchCountFunc;
import stc.InputDStreamValueMapper;
import stc.STCCliParser;
import tc.*;
import java.util.*;

public class StreamingTCFinder {

    private static String outputDir = "";
    private static double distanceThreshold = 0.0001;   //eps
    private static int densityThreshold = 3;            //mu
    private static int timeInterval = 50;               //T
    private static int durationThreshold = 2;           //k
    private static int numSubPartitions = 1;            //n
    private static int sizeThreshold = 2;
    private static boolean debugMode = false;

    public static void main(String[] args) throws Exception {

        // setup the cli parser
        STCCliParser parser = new STCCliParser(args);
        parser.parse();

        if(parser.getCmd() == null) {
            System.exit(1);
        }
        initParams(parser);

        // setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        SparkConf sparkConf = new SparkConf()
                .setAppName("StreamingTCFinder");
        if(debugMode) sparkConf.setMaster("local[*]");

        int batchInterval = Integer.parseInt(propertyParser.getProperty(Config.SPARK_BATCH_INTERVAL));
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
        ssc.checkpoint(propertyParser.getProperty(Config.SPARK_CHECKPOINT_DIR));

        Set<String> topics = new HashSet(Arrays.asList(
                propertyParser.getProperty(Config.KAFKA_TOPICS).split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", propertyParser.getProperty(Config.KAFKA_BROKERS));

        // create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> inputDStream =
                KafkaUtils.createDirectStream(
                        ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class,
                        kafkaParams, topics);

        JavaPairDStream windowedInputRDD = inputDStream.window(Durations.seconds(batchInterval));
        JavaDStream<String> lines = windowedInputRDD.
                map(new InputDStreamValueMapper());
        lines.foreach(new BatchCountFunc());

        // partition the entire common.data set into trajectory slots
        // format: <slot_id, { pi, pj,... }>
        JavaPairDStream<Long, Iterable<TCPoint>> slotsRDD =
                lines.mapToPair(new stc.TrajectorySlotMapper())
                        .groupByKey();

        // partition each slot into sub-partitions
        // format: <slot_id, TCRegion>
        JavaDStream<Tuple2<Long, TCRegion>> subPartitionsRDD =
                slotsRDD.flatMap(new KDTreeSubPartitionMapper(numSubPartitions)).cache();

        // get each point per partition
        // format: <(slotId, regionId), <objectId, point>>
        JavaPairDStream<String, Tuple2<Integer, TCPoint>> pointsRDD =
                subPartitionsRDD.flatMapToPair(new SubPartitionToPointsFlatMapper());

        // get all polylines per partition
        // format: <(slotId, regionId), {<objectId, polyline>}
        final JavaPairDStream<String, Map<Integer, TCPolyline>> polylinesRDD =
                subPartitionsRDD.mapToPair(new SubPartitionToPolylinesMapper());

        // get density reachable per sub partition
        // format: <(slotId, regionId, objectId), {objectId}>
        JavaPairDStream<String, Iterable<Integer>> densityReachableRDD =
                pointsRDD.transformToPair(new Function2<JavaPairRDD<String, Tuple2<Integer, TCPoint>>, Time, JavaPairRDD<String, Iterable<Integer>>>() {
                    @Override
                    public JavaPairRDD<String, Iterable<Integer>> call(JavaPairRDD<String, Tuple2<Integer, TCPoint>> stringTuple2JavaPairRDD, Time time) throws Exception {
                        return null;
                    }
                });


                pointsRDD.join(polylinesRDD)
                        .flatMapToPair(new stc.CoverageDensityReachableMapper(distanceThreshold))
                        .groupByKey().filter(new CoverageDensityReachableFilter(densityThreshold));

        // remove objectId from key
        // format: <(slotId, regionId), {objectId}>
        JavaPairDStream<String, Iterable<Integer>> densityConnectionRDD
                = densityReachableRDD
                .mapToPair(new SubPartitionRemoveObjectIDMapper());

        // merge density connection sub-partitions
        // format: <(slotId, regionId), {{objectId}}>
        JavaPairDStream<String, Iterable<Integer>> subpartMergeConnectionRDD =
                densityConnectionRDD
                        .reduceByKey(new CoverageDensityConnectionReducer());

        // remove regionId from key
        // format: <slotId, {objectId}>
        JavaPairDStream<Integer, Iterable<Integer>> slotConnectionRDD =
                subpartMergeConnectionRDD
                        .mapToPair(new SlotRemoveSubPartitionIDMapper())
                        .reduceByKey(new CoverageDensityConnectionReducer());

        JavaPairDStream<Integer, Iterable<Integer>> windowedSlotConnectionRDD =
                slotConnectionRDD.window(Durations.seconds(batchInterval * durationThreshold));

        // obtain trajectory companion
        // format: <{objectId}, {slotId}>
        JavaPairDStream<String, Iterable<Integer>> companionRDD =
                windowedSlotConnectionRDD
                        .flatMapToPair(new CoverageDensityConnectionSubsetMapper(sizeThreshold))
                        .mapToPair(new CoverageDensityConnectionMapper())
                        .groupByKey()
                        .filter(new TrajectoryCompanionFilter(durationThreshold));

        if(debugMode)
            companionRDD.print();
        else
            companionRDD.saveAsHadoopFiles(outputDir, "csv",
                    String.class, String.class, TextOutputFormat.class);

        ssc.start();
        ssc.awaitTermination();
    }

    private static void initParams(STCCliParser parser) {
        String foundStr = CliParserBase.ANSI_GREEN + "param -%s is set. Use custom value: %s" + STCCliParser.ANSI_RESET;
        String notFoundStr = CliParserBase.ANSI_RED + "param -%s not found. Use default value: %s" + STCCliParser.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {
            // output
            if (cmd.hasOption(STCCliParser.OPT_STR_OUTPUTDIR)) {
                outputDir = cmd.getOptionValue(STCCliParser.OPT_STR_OUTPUTDIR);
            } else {
                System.err.println("Output directory not defined. Aborting...");
                parser.help();
            }

            // debug
            if (cmd.hasOption(STCCliParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // distance threshold
            if (cmd.hasOption(STCCliParser.OPT_STR_DISTTHRESHOLD)) {
                distanceThreshold = Double.parseDouble(cmd.getOptionValue(STCCliParser.OPT_STR_DISTTHRESHOLD));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_DISTTHRESHOLD, distanceThreshold));
            }

            // density threshold
            if (cmd.hasOption(STCCliParser.OPT_STR_DENTHRESHOLD)) {
                densityThreshold = Integer.parseInt(cmd.getOptionValue(STCCliParser.OPT_STR_DENTHRESHOLD));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_DENTHRESHOLD, densityThreshold));
            }

            // time interval
            if (cmd.hasOption(STCCliParser.OPT_STR_TIMEINTERVAL)) {
                timeInterval = Integer.parseInt(cmd.getOptionValue(STCCliParser.OPT_STR_TIMEINTERVAL));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_TIMEINTERVAL, timeInterval));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_TIMEINTERVAL, timeInterval));
            }

            // life time
            if (cmd.hasOption(STCCliParser.OPT_STR_LIFETIME)) {
                durationThreshold = Integer.parseInt(cmd.getOptionValue(STCCliParser.OPT_STR_LIFETIME));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_LIFETIME, durationThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_LIFETIME, durationThreshold));
            }

            // number of  sub-partitions
            if (cmd.hasOption(STCCliParser.OPT_STR_NUMPART)) {
                numSubPartitions = Integer.parseInt(cmd.getOptionValue(STCCliParser.OPT_STR_NUMPART));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_NUMPART, numSubPartitions));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_NUMPART, numSubPartitions));
            }

            // size threshold
            if (cmd.hasOption(STCCliParser.OPT_STR_SIZETHRESHOLD)) {
                sizeThreshold = Integer.parseInt(cmd.getOptionValue(STCCliParser.OPT_STR_SIZETHRESHOLD));
                System.out.println(String.format(foundStr,
                        STCCliParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            } else {
                System.out.println(String.format(notFoundStr,
                        STCCliParser.OPT_STR_SIZETHRESHOLD, sizeThreshold));
            }
        } catch (NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(1);
        }
    }
}
