package apps;

import common.geometry.TCPoint;
import common.geometry.TCPolyline;
import common.geometry.TCRegion;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import tc.*;

import java.util.*;

/**
 * Created by kevinkim on 2016-04-22.
 */

public class StreamingTCFinder {

    private static String inputFilePath = "";
    private static String outputDir = "";
    private static double distanceThreshold = 0.0001;   //eps
    private static int densityThreshold = 3;            //mu
    private static int timeInterval = 50;               //T
    private static int durationThreshold = 3;           //k
    private static int numSubPartitions = 2;            //n
    private static int sizeThreshold = 2;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: StreamingTCFinder <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("StreamingTCFinder");

        sparkConf.setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        /* could be used to apply tcf on all rdds of stream...TODO : compare with what I did so far
        dStream.foreachRDD {
            windowRdd => {
                if (!windowRdd.isEmpty) processWindow(windowRdd.cache())
            }
        }*/

        JavaDStream<String> lines = messages.map(m -> m._2());

        // partition the entire common.data set into trajectory slots
        // format: <slot_id, { pi, pj,... }>
        JavaPairDStream<Integer, Iterable<TCPoint>> slotsRDD =
                lines.mapToPair(new TrajectorySlotMapper(timeInterval))
                         //.partitionBy(new HashPartitioner(numSubPartitions))
                        .groupByKey();

        slotsRDD.count().print();
        // partition each slot into sub-partitions
        // format: <slot_id, TCRegion>
        JavaDStream<Tuple2<Integer, TCRegion>> subPartitionsRDD =
                slotsRDD.flatMap(new KDTreeSubPartitionMapper(numSubPartitions)).cache();
        // get each point per partition
        // format: <(slotId, regionId), <objectId, point>>
        JavaPairDStream<String, Tuple2<Integer, TCPoint>> pointsRDD =
                subPartitionsRDD.flatMapToPair(new SubPartitionToPointsFlatMapper());

        // get all polylines per partition
        // format: <(slotId, regionId), {<objectId, polyline>}
        JavaPairDStream<String, Map<Integer, TCPolyline>> polylinesRDD =
                subPartitionsRDD.mapToPair(new SubPartitionToPolylinesMapper());

        // get density reachable per sub partition
        // format: <(slotId, regionId, objectId), {objectId}>
        JavaPairDStream<String, Iterable<Integer>> densityReachableRDD =
                pointsRDD.join(polylinesRDD)
                        .flatMapToPair(new CoverageDensityReachableMapper(distanceThreshold))
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

        // obtain trajectory companion
        // format: <{objectId}, {slotId}>
        JavaPairDStream<String, Iterable<Integer>> companionRDD =
                slotConnectionRDD
                        .flatMapToPair(new CoverageDensityConnectionSubsetMapper(sizeThreshold))
                        .mapToPair(new CoverageDensityConnectionMapper())
                        .groupByKey()
                        .filter(new TrajectoryCompanionFilter(durationThreshold));

        //If Iteration found
        //Desired Results from Trajectories1.txt
        //(2,12,[8, 1, 5, 2, 3])
        //(2,81,[8, 9, 10])
        companionRDD.saveAsHadoopFiles("hdfs://127.0.0.1:8020/user/spark/TCF","csv",
                String.class, String.class, (Class) TextOutputFormat.class);

        //JavaDStream<Long> count = companionRDD.count().reduce( (p1, p2) -> (p1 + p2));


        jssc.start();
        jssc.awaitTermination();



    }
}
