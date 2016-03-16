package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CrowdObjectToTimestampMapper implements
        PairFlatMapFunction<Tuple2<Iterable<Tuple2<Integer, Cluster>>, Long>,
                String, Integer>
{
    @Override
    public Iterable<Tuple2<String, Integer>> call(Tuple2<Iterable<Tuple2<Integer, Cluster>>, Long> input) throws Exception {
        return apply(input);
    }

    public Iterable<Tuple2<String, Integer>> apply(Tuple2<Iterable<Tuple2<Integer, Cluster>>, Long> input) {
        List<Tuple2<String, Integer>> result = new ArrayList();
        for (Tuple2<Integer, Cluster> t : input._1()) {
            List<TCPoint> points = t._2().getPoints();
            for (TCPoint point : points) {
                result.add(new Tuple2(
                        String.format("%s,%s", input._2(), point.getObjectId()),
                        point.getTimeStamp()
                ));
            }
        }
        // <(crowdId, objectId), timestamp>
        return result;
    }
}
