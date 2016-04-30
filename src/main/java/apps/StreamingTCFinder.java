package apps;

import common.cli.Config;
import common.cli.PropertyFileParser;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

public class StreamingTCFinder {

    public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            System.err.println("USAGE: <propsfile>");
            System.exit(1);
        }

        // Setup the property parser
        PropertyFileParser propertyParser = new PropertyFileParser(args[0]);
        propertyParser.parseFile();

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("StreamingTCFinder");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

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

        // TO-DO

        ssc.start();
        ssc.awaitTermination();
    }
}
