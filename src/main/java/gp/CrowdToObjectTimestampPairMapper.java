package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CrowdToObjectTimestampPairMapper implements
        PairFlatMapFunction<Tuple2<Iterable<Tuple2<Integer,Cluster>>,Long>,
                Long, Iterable<Tuple2<Integer, Integer>>>
{
    @Override
    public Iterable<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> call(
            Tuple2<Iterable<Tuple2<Integer, Cluster>>, Long> input) throws Exception {
        return apply(input);
    }

    public Iterable<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> apply(
            Tuple2<Iterable<Tuple2<Integer, Cluster>>, Long> input) {
        List<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> result = new ArrayList();

        for (Tuple2<Integer, Cluster> t : input._1()) {
            List<TCPoint> points = t._2().getPoints();
            List<Tuple2<Integer,Integer>> pointList = new ArrayList();
            for (TCPoint point : points) {
                pointList.add(new Tuple2(
                        point.getObjectId(),
                        point.getTimeStamp()));
            }
            result.add(new Tuple2(input._2(), pointList));
        }
        return result;
    }
}
