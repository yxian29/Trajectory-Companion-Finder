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
import sg.SGCmdParser;
import sg.TimestampFilter;
import sg.TimestampMapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class KafkaStreamGenerator {

    private static String topic = "";
    private static String inputFile = "";
    private static int timeSlotDuration = 100;
    private static int timeScale = 10;
    private static String broker = "";
    private static boolean debugMode = true;

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
            producer.send(messageSequence.get(i));
            Thread.sleep(publishInterval);

        }

    }

    public void createMessageSequence() {

        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreamGenerator");

        if (debugMode) sparkConf.setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> trajectories = ctx.textFile(inputFile);
        totalTrajectoriesCount = trajectories.count();

        JavaPairRDD<Integer, String> sortedTrajectories = trajectories
                .mapToPair(new TimestampMapper())
                .reduceByKey((t1, t2) -> t1 + " - " + t2)
                .sortByKey(true);

        //Calculate the stream duration  in order to partition into time slot
        int streamStart = sortedTrajectories.first()._1();
        int streamEnd = sortedTrajectories.sortByKey(false).first()._1();
        int streamDuration = streamEnd - streamStart;
        int extraSlot = (streamDuration % timeSlotDuration == 0) ? 0 : 1;
        int timeSlotCount = streamDuration / timeSlotDuration + extraSlot;
        int currTimeSlotStart;
        int currTimeSlotEnd;

        long partitionedTrajectoryCount = 0;

        boolean successfulPartitioning = false;
        int retries = 0;


        ctx.broadcast(messageSequence);

        //need to find an alternative to for loop, for loop over collect is a huge bottleneck on performance!
        for (int i = 1; i <= timeSlotCount; i++) {

            currTimeSlotStart = (i - 1) * timeSlotDuration + streamStart;
            currTimeSlotEnd = (i * timeSlotDuration) + streamStart;

            JavaRDD<String> trajectoriesInTimeSlot =
                    trajectories.filter(new TimestampFilter(currTimeSlotStart, currTimeSlotEnd));

            currentTimeSlotTrajectories = new ArrayList<>();

            List<KeyedMessage<Integer, String>> keyedMessageList =
                    trajectoriesInTimeSlot.map(t -> new KeyedMessage<Integer, String>(topic, t))
                            .collect();

            currentTimeSlotTrajectories = keyedMessageList;
            partitionedTrajectoryCount = partitionedTrajectoryCount + keyedMessageList.size();

            messageSequence.add(currentTimeSlotTrajectories);

        }

        System.out.println("   Partitioned all trajectories into [ " + messageSequence.size() + " ] slots");
        System.out.println("   Input file(s) trajectory count was: " + totalTrajectoriesCount);
        System.out.println("   # of trajectories partitioned     : " + partitionedTrajectoryCount);


        ctx.stop();
    }


    public static void main(String[] args) throws Exception {

        SGCmdParser parser = new SGCmdParser(args);
        parser.parse();

        if(parser.getCmd() == null) {
            System.exit(0);
        }

        initParams(parser);

        KafkaStreamGenerator streamGenerator = new KafkaStreamGenerator();

        // Plan Sequence of Msg from input file
        streamGenerator.createMessageSequence();

        // Initialize producer
        streamGenerator.initializeProducer();

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

            // timeSlotDuration
            if (cmd.hasOption(SGCmdParser.OPT_STR_WINDOW_DURATION)) {
                timeSlotDuration= Integer.parseInt(cmd.getOptionValue(SGCmdParser.OPT_STR_WINDOW_DURATION));
                System.out.println(String.format(foundStr,
                        SGCmdParser.OPT_STR_WINDOW_DURATION, timeSlotDuration));
            } else {
                System.out.println(String.format(notFoundStr,
                        SGCmdParser.OPT_STR_WINDOW_DURATION, timeSlotDuration));
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


