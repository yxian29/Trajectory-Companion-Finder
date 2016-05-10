package apps;

import common.cmd.CmdParserBase;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sg.KeyedMessageMapper;
import sg.SGCmdParser;
import sg.TimestampMapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class KafkaStreamGenerator {

    private static String topic = "";
    private static String inputFile = "";
    private static int timeScale = 1;
    private static String broker = "";
    private static boolean debugMode = true;

    private static List<KeyedMessage< Integer,String >> currentTimeSlotTrajectories;
    //private static List<List<KeyedMessage< Integer,String >>> messageSequence = new ArrayList<>();
    private JavaPairRDD<Integer, List<KeyedMessage<String, String>>> messageSequence;
    private List <Tuple2<Integer, List<KeyedMessage<String, String>>>> messageList = new ArrayList<>();
    private static Producer<String, String> producer;
    private static long totalTrajectoriesCount;

    public void initializeProducer(){

        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", broker);
        producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<String, String>(producerConfig);

    }


    public void publishMessage() throws InterruptedException {

        System.out.print("Start of Message publishing at a time speed of " + timeScale + "x ");
        int publishInterval=0;

        int msgSeqSize = messageList.size();
        List<KeyedMessage< String,String >> message;
        for(int i = 0; i < msgSeqSize; i++) {

            //Calculate time for sleep between this msg and the next
            publishInterval = (messageList.get(i+1)._1() - messageList.get(i)._1()) * 1000 / timeScale ;

            message = new ArrayList<>();
            message = messageList.get(i)._2();

            //Send List of locationPoints
            Date date = new Date();
            producer.send(message);
            System.out.println("["+ date +"] Size of the message Sent: " + messageList.get(i)._2().size());
            System.out.println("    Next Message in [" + publishInterval + "] ms.");
            Thread.sleep(publishInterval);
        }
    }

    public void createMessageSequence(){

        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamGenerator");

        if (debugMode) sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> locationPoints = ctx.textFile(inputFile);
        totalTrajectoriesCount = locationPoints.count();


        //Group entries by timestamps
        //Reduce objects with same timestamps
        //Sort in ascending order of time
        //eg.   if timestamp is 5 and there is 3 position from 3 objects:
        //      <5, "1,2,3,5-2,3,4,5-3,24,3,5">
        JavaPairRDD<Integer, String> locationPointReduceByTimestamp = locationPoints
                .mapToPair(new TimestampMapper())
                .reduceByKey((t1, t2) -> t1 + "-" + t2)
                .sortByKey(true);

        //Map each timestamp values to a keyedMessage format needed for the producer
        //eg.   1,2,3,5-2,3,4,5-3,24,3,5 ->
        //      KeyedMsg(topic,"1,2,3,5") , KeyedMsg(topic,"2,3,4,5") KeyedMsg(topic,"3,24,3,5")
         messageSequence =
                locationPointReduceByTimestamp.mapToPair(new KeyedMessageMapper(topic));

        messageList = messageSequence.collect();

        ctx.stop();

        //Print number of objects per timestamp
        messageList.forEach(msg -> System.out.print(msg._1()+ " - list size: " + msg._2()+ "\n"));
    }


    public static void main(String[] args) throws Exception {

        SGCmdParser parser = new SGCmdParser(args);
        parser.parse();

        if(parser.getCmd() == null) {
            System.exit(0);
        }

        initParams(parser);

        KafkaStreamGenerator streamGenerator = new KafkaStreamGenerator();

        // Initialize producer
        streamGenerator.initializeProducer();

        // Create Sequence of Msg from input file
        streamGenerator.createMessageSequence();

        // Publish message
        streamGenerator.publishMessage();

        //Close the producer
        producer.close();
    }

    private static void initParams(SGCmdParser parser)
    {
        String foundStr = CmdParserBase.ANSI_GREEN + "param -%s is set. Use custom value: %s" + SGCmdParser.ANSI_RESET;
        String notFoundStr = CmdParserBase.ANSI_RED + "param -%s not found. Use default value: %s" + SGCmdParser.ANSI_RESET;
        CommandLine cmd = parser.getCmd();

        try {

            // debug
            if (cmd.hasOption(SGCmdParser.OPT_STR_DEBUG)) {
                debugMode = true;
                System.out.println("Enter debug mode. master forces to be local");
            }

            // topic
            if (cmd.hasOption(SGCmdParser.OPT_STR_TOPIC)) {
                topic = cmd.getOptionValue(SGCmdParser.OPT_STR_TOPIC);
                System.out.println(String.format(foundStr,
                        SGCmdParser.OPT_STR_TOPIC, topic ));
            } else {
                System.err.println("Topic not defined. Aborting...");
                parser.help();
            }

            // broker
            if (cmd.hasOption(SGCmdParser.OPT_STR_BROKER)) {
                broker = cmd.getOptionValue(SGCmdParser.OPT_STR_BROKER);
                System.out.println(String.format(foundStr,
                        SGCmdParser.OPT_STR_BROKER, broker ));
            } else {
                System.err.println("Broker not defined. Aborting...");
                parser.help();
            }

            // timescale
            if (cmd.hasOption(SGCmdParser.OPT_STR_TIMESCALE)) {
                timeScale = Integer.parseInt(cmd.getOptionValue(SGCmdParser.OPT_STR_TIMESCALE));
                System.out.println(String.format(foundStr,
                        SGCmdParser.OPT_STR_TIMESCALE, timeScale));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGCmdParser.OPT_STR_TIMESCALE, timeScale));
            }

            // input
            if (cmd.hasOption(SGCmdParser.OPT_STR_INPUTFILE)) {
                inputFile = cmd.getOptionValue(SGCmdParser.OPT_STR_INPUTFILE);
                System.out.println(String.format(foundStr,
                        SGCmdParser.OPT_STR_INPUTFILE, inputFile));
            } else {
                System.err.println("Input file not defined. Aborting...");
                parser.help();
            }

        }
        catch(NumberFormatException e) {
            System.err.println(String.format("Error parsing argument. Exception: %s", e.getMessage()));
            System.exit(0);
        }
    }

}


