package common.cli;

public class Config {
    // kakfa
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_TOPICS = "kafka.topics";

    // spark
    public static final String SPARK_BATCH_INTERVAL = "spark.streaming.batch.duration";
    public static final String SPARK_CHECKPOINT_DIR = "spark.checkpoint.directory";
}
