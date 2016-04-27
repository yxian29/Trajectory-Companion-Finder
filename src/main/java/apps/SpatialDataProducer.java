package apps;

/**
 * Created by kevinkim on 2016-04-26.
 * REFERENCE:
 */

import common.geometry.TCPoint;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import sdp.TimestampFiler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SpatialDataProducer {

    private static Producer<Integer, String> producer;
    private static JavaPairRDD<Integer, Iterable<TCPoint>> testRDD;
    private static final String topic= "SpatialData"; // TODO Find out what is final
    private static final Logger LOG = Logger.getLogger(SpatialDataProducer.class);

    //TODO add passable parameters
    private static String inputFilePath = "C:\\Users\\kevinkim\\IdeaProjects\\Streaming-TCFinder\\data\\dataset_1\\Trajectories1.txt";
    private static int timeInterval = 20;

    private static List<KeyedMessage< Integer,String >> currentTimeSlotTrajectories = new ArrayList<>();
    private static List<List<KeyedMessage< Integer,String >>> messageSequence = new ArrayList<>();

    public void initializeProducer(){
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", "localhost:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<Integer, String>(producerConfig);

    }

    public void publishMesssage() throws Exception{
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        System.out.print("Starting Message publication to Kafka broker...");

        /*
        String msg = null;
        //msg = reader.readLine(); // Read message from console
        String msg1 = "39.984688,116.318385,20";
        String msg2 = "39.984563,116.317517,40";
        String msg3 = "39.984586,116.316716,60";

        String[] trajectoryArray = { msg1,msg2,msg3 };


        for(int i=0; i < trajectoryArray.length; i++){

            msg = trajectoryArray[i];
            currentTimeSlotTrajectories.add(new KeyedMessage<Integer, String>(topic, msg));
        }
        */
        //List<KeyedMessage< Integer,String >>  keyedMsg = Arrays.asList(new KeyedMessage<Integer, String>(topic, msg1), new KeyedMessage<Integer, String>(topic, msg2));
        List<KeyedMessage< Integer,String >>  keyedMsg = currentTimeSlotTrajectories;
        //Define topic name and message

        //List<KeyedMessage< Integer,String >> keyedMsg = messages;
        producer.send(keyedMsg); // This publishes message on given topic


        System.out.println("--> Message sent @ time ##:##:## \n[" + keyedMsg + "] "+"\nCheck message on Consumer's program console");


    }

    public void getTrajectoriesInTimeslot(){

        System.out.print("[createMessageSequence]");
        SparkConf sparkConf = new SparkConf().setAppName("SpatialDataProducer");

        // force to local mode if it is debug
        sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> trajectoryEntries = ctx.textFile(inputFilePath);

        // Defining timeSlot upper and lower bound range
        int[] timeSlotRange = { 0, timeInterval };


        //Collect all lines that contain a specific timestamp if it is a range, apply the range filter
        //e.g collect lines where 30 > timestamp >=20 will returns lines with
        JavaRDD<String> trajectoriesInTimeSlot = trajectoryEntries.filter(new TimestampFiler(timeSlotRange));

        trajectoriesInTimeSlot.foreach(line -> currentTimeSlotTrajectories.add(
                new KeyedMessage<Integer, String>(topic, line)));

    }

    public void initializeSparkContext(){

    }

    public void getDataStreamDuration(){

    }


    public static void main(String[] args) throws Exception {

        SpatialDataProducer kafkaProducer = new SpatialDataProducer();

        // Plan Sequence of Msg from input file
        kafkaProducer.getTrajectoriesInTimeslot();

        // Initialize producer
        kafkaProducer.initializeProducer();
        // Publish message
        kafkaProducer.publishMesssage();
        //Close the producer
        producer.close();
    }


}


