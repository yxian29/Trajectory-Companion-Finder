package apps;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import sdp.TimestampFilter;
import sdp.TimestampMapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class KafkaStreamGenerator {

    private static String topic = "SpatialData3";
    private static String inputFilePath = "C:\\Users\\kevinkim\\IdeaProjects\\Streaming-TCFinder\\data\\dataset_2\\Trajectory_4.txt";
    private static int timeSlotDuration = 50;
    private static int timeScale = 10;
    private static String broker = "localhost:9092";
    private static boolean debugMode = true;
    private static boolean forceNoDataLoss = false; //set to false if you don't mind losing a few trajectories.
    private static int maxRetries = 3;

    private static List<KeyedMessage< Integer,String >> currentTimeSlotTrajectories;
    private static List<List<KeyedMessage< Integer,String >>> messageSequence = new ArrayList<>();
    private static Producer<Integer, String> producer;
    private static long totalTrajectoriesCount;

    public void initializeProducer(){
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", broker);
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<Integer, String>(producerConfig);


    }

    public void publishMessage() throws InterruptedException {

        long publishInterval = (timeSlotDuration / timeScale) * 1000;

        System.out.print("Start of Message publishing at an interval of [ " + publishInterval/1000
                + "s ] with a time scale of [ "+ timeScale +"x ]\n" );

        for (int i = 0; i < messageSequence.size(); i++) {
            Date date = new Date();
            System.out.print("[" + date + "] Sending Msg ( " + (i + 1) + "/"
                    + messageSequence.size() + " ) containing ( " + messageSequence.get(i).size()
                    +  "/" + totalTrajectoriesCount + " ) trajectories\n");
            //producer.send(messageSequence.get(i));
            Thread.sleep(publishInterval);

        }

    }

    public void createMessageSequence(){

        System.out.println("  Creating Message Sequence");

        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamGenerator");

        if (debugMode) sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> trajectories = ctx.textFile(inputFilePath);
        totalTrajectoriesCount = trajectories.count();

        JavaPairRDD<Integer,String> sortedTrajectories= trajectories
                .mapToPair(new TimestampMapper())
                .reduceByKey((t1,t2)-> t1+" - "+t2)
                .sortByKey(true);

        //Calculate the stream duration  in order to partition into time slot
        int streamStart = sortedTrajectories.first()._1();
        int streamEnd = sortedTrajectories.sortByKey(false).first()._1();
        int streamDuration = streamEnd - streamStart;
        int extraSlot = (streamDuration % timeSlotDuration == 0) ? 0: 1;
        int timeSlotCount = streamDuration / timeSlotDuration + extraSlot;
        int currTimeSlotStart;
        int currTimeSlotEnd;

        long partitionedTrajectoryCount = 0;

        boolean successfulPartitioning = false;
        int retries = 0;

        while( !successfulPartitioning && retries < maxRetries) {

            for (int i = 1; i <= timeSlotCount; i++) {

                currTimeSlotStart = (i - 1) * timeSlotDuration + streamStart;
                currTimeSlotEnd = (i * timeSlotDuration) + streamStart;

                JavaRDD<String> trajectoriesInTimeSlot =
                        trajectories.filter(new TimestampFilter(currTimeSlotStart, currTimeSlotEnd));

                currentTimeSlotTrajectories = new ArrayList<>();

                trajectoriesInTimeSlot.foreach(t -> currentTimeSlotTrajectories.add(
                        new KeyedMessage<Integer, String>(topic, t)));

                partitionedTrajectoryCount = partitionedTrajectoryCount + currentTimeSlotTrajectories.size();

                messageSequence.add(currentTimeSlotTrajectories);

            }

            System.out.println("   Partitioned all trajectories into [ " + messageSequence.size() + " ] slots");
            System.out.println("   Input file(s) trajectory count was: " + totalTrajectoriesCount);
            System.out.println("   # of trajectories partitioned     : " + partitionedTrajectoryCount);

            //Note: For some unknown reason, some trajectories (1 or 2 at most) are being loss need to figure out why.
            //Source of data loss is most likely TimestampFilter
            if (totalTrajectoriesCount == partitionedTrajectoryCount || !forceNoDataLoss) {
                successfulPartitioning = true;
                System.out.print("Successfully created message sequence!");
            } else {
                System.out.print("[Warn] Some trajectories were loss for unknown reason; Re-creating MessageSequence.");
                messageSequence = new ArrayList<>();
                partitionedTrajectoryCount = 0;
                retries++;

            }

            if (retries > maxRetries) System.exit(1);
        }
        ctx.stop();
    }


    public static void main(String[] args) throws Exception {

        KafkaStreamGenerator streamGenerator = new KafkaStreamGenerator();

        // Plan Sequence of Msg from input file
        streamGenerator.createMessageSequence();

        // Initialize producer
       // streamGenerator.initializeProducer();

        // Publish message
        streamGenerator.publishMessage();

        //Close the producer
        //producer.close();
    }


}


