package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.HashSet;
import java.util.Set;

public class CrowdObjectInvertIndexer implements
        PairFlatMapFunction<Tuple2<Integer,Iterable<Cluster>>,
                String, Cluster> {
    @Override
    public Iterable<Tuple2<String, Cluster>> call(Tuple2<Integer, Iterable<Cluster>> input) throws Exception {
        return getObjectClusterPair(input);
    }

    public Set<Tuple2<String, Cluster>> getObjectClusterPair(
            Tuple2<Integer, Iterable<Cluster>> input) {

        Set<Tuple2<String, Cluster>> result = new HashSet<>();

        int crowdId = input._1();
        Iterable<Cluster> clusters = input._2();

        for (Cluster cluster: clusters) {
            Iterable<TCPoint> points = cluster.getPoints();
            for (TCPoint p: points) {

                result.add(new Tuple2<>(
                        String.format("%s,%s", crowdId, p.getObjectId()),
                        cluster
                ));
            }
        }
        return result;
    }
}
