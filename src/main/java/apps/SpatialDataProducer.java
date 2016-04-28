package apps;

/**
 * Created by kevinkim on 2016-04-26.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import sdp.TimestampFilter;
import sdp.TimestampMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class SpatialDataProducer {

    private static final Logger LOG = Logger.getLogger(SpatialDataProducer.class);
    private static Producer<Integer, String> producer;
    private static String topic= "SpatialData3"; // TODO Find out what is final

    //TODO add passable parameters
    private static String inputFilePath = "C:\\Users\\kevinkim\\IdeaProjects\\Streaming-TCFinder\\data\\dataset_1\\Trajectories1.txt";
    private static int timeSlotIDuration = 50;
    private static int timeScale = 10;

    private static List<KeyedMessage< Integer,String >> currentTimeSlotTrajectories = new ArrayList<>();
    private static List<List<KeyedMessage< Integer,String >>> messageSequence = new ArrayList<>();
    private static int streamDuration;

    public void initializeProducer(){
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", "localhost:9092");
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<Integer, String>(producerConfig);

    }

    public void publishMesssage() throws InterruptedException {
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

        //List<KeyedMessage< Integer,String >>  keyedMsg = Arrays.asList(new KeyedMessage<Integer, String>(topic, msg1), new KeyedMessage<Integer, String>(topic, msg2));
        List<KeyedMessage< Integer,String >>  keyedMsg = currentTimeSlotTrajectories;
        //Define topic name and message

        //List<KeyedMessage< Integer,String >> keyedMsg = messages;
        producer.send(keyedMsg); // This publishes message on given topic
*/
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

        for (int i = 0; i < messageSequence.size(); i++) {
            Date date = new Date();
            System.out.print("[" + date + "] Sending Msg " + i + "...");
            producer.send(messageSequence.get(i));
            System.out.println("(Check message on Consumer's program console)");
            Thread.sleep(15000);
        }

    }

    public void getTrajectoriesInTimeslot(){
/*
        // Defining timeSlot upper and lower bound range
        int[] timeSlotRange = { 0, timeInterval };

        //Collect all lines that contain a specific timestamp if it is a range, apply the range filter
        //e.g collect lines where 30 > timestamp >=20 will returns lines with
        JavaRDD<String> trajectoriesInTimeSlot = trajectoryEntries.filter(new TimestampFilter(timeSlotRange));

        trajectoriesInTimeSlot.foreach(line -> currentTimeSlotTrajectories.add(
                new KeyedMessage<Integer, String>(topic, line)));
*/
    }

    public void createMessageSequence(){

        SparkConf sparkConf = new SparkConf().setAppName("SpatialDataProducer");
        sparkConf.setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> trajectoryEntries = ctx.textFile(inputFilePath);

        System.out.println("NumberOflinesInInputFile: " + trajectoryEntries.count());
        JavaPairRDD<Integer,String> sortedTrajectories= trajectoryEntries.mapToPair(new TimestampMapper())
                .reduceByKey((t1,t2)-> t1+" - "+t2)
                .sortByKey(true);

        int lowerBound = sortedTrajectories.first()._1();
        int upperBound = sortedTrajectories.sortByKey(false).first()._1();

        //Split the duration in 10 smaller time intervals in order to send only 10 groups of messages
        //This is a temporary implementation until further discuss the desired behaviour/usage of the generator.
        streamDuration = upperBound - lowerBound;
        int currentIterationLowerBound ;
        int currentIterationUpperBound ;
        for( int i=1; i<=10; i++){

            //FIX BOUNDARIES
            currentIterationLowerBound = ( i - 1 ) * Math.round(streamDuration/10)  + 20;
            if (i < 10) {
                currentIterationUpperBound = (i * (streamDuration / 10));
            } else {
                currentIterationUpperBound = (upperBound) + 1;
            }

            System.out.print("MSG " + i+ " --> Current Trajectories are timestamped between [ "
                     + currentIterationLowerBound + " - " + currentIterationUpperBound + " ]");
            // Defining timeSlot upper and lower bound range
            int[] timeSlotRange = { currentIterationLowerBound, currentIterationUpperBound};

            //Collect all lines that contain a specific timestamp if it is a range, apply the range filter
            //e.g collect lines where 30 > timestamp >=20 will returns lines with
            JavaRDD<String> trajectoriesInTimeSlot = trajectoryEntries.filter(new TimestampFilter(timeSlotRange));

            currentTimeSlotTrajectories= new ArrayList<>();

            System.out.println("-->BEFORE currentTSTraj.Size: " + currentTimeSlotTrajectories.size());

            trajectoriesInTimeSlot.foreach( trajectory -> currentTimeSlotTrajectories.add(
                    new KeyedMessage<Integer, String>(topic, trajectory)));

            System.out.println("-->AFTER currentTSTraj.Size: " + currentTimeSlotTrajectories.size());

            messageSequence.add(currentTimeSlotTrajectories);

        }

            System.out.println("messageSequence.size "+ messageSequence.size());
        ctx.stop();
    }

    public void setMessageInterval(){

        int timeScale;
        int timeSlotDuration;

    }


    public static void main(String[] args) throws Exception {

        SpatialDataProducer streamGenerator = new SpatialDataProducer();

        // Plan Sequence of Msg from input file
        streamGenerator.createMessageSequence();

        // Initialize producer
        streamGenerator.initializeProducer();

        // Publish message
        streamGenerator.publishMesssage();

        //Close the producer
        producer.close();
    }


}


