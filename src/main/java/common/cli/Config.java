package common.cli;

public class Config {
    // kakfa
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_TOPICS = "kafka.topics";

    // spark
    public static final String SPARK_BATCH_INTERVAL = "spark.streaming.batch.duration";
    public static final String SPARK_CHECKPOINT_DIR = "spark.checkpoint.directory";

    // brenchmark
    public static final String BM_ENABLE_PARTITION = "partition.enable";
    public static final String BM_JOIN_METHOD = "join.method";
    public static final int BM_VAL_MAP_JOIN = 1;
    public static final int BM_VAL_REDUCE_JOIN = 2;
}
