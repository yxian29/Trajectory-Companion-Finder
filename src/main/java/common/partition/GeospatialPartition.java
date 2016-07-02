package common.partition;

import org.apache.spark.api.java.JavaPairRDD;

public interface GeospatialPartition<R, U, V> {
    JavaPairRDD<R, Iterable<V>> apply(JavaPairRDD<U, V> source);
}
