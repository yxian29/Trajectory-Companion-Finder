package gp;

import common.data.Crowd;
import common.data.DBSCANCluster;
import common.data.TCPoint;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CrowdToObjectTimestampPairMapper implements
        PairFlatMapFunction<Tuple2<Tuple2<Integer, Crowd>, Long>,
                Long, Iterable<Tuple2<Integer, Integer>>>
{
    @Override
    public Iterable<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> call(
            Tuple2<Tuple2<Integer, Crowd>, Long> input) throws Exception {
        return apply(input);
    }

    public Iterable<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> apply(
            Tuple2<Tuple2<Integer, Crowd>, Long> input) {
        List<Tuple2<Long, Iterable<Tuple2<Integer, Integer>>>> result = new ArrayList();

        long crowdId = input._2();
        Crowd crowd = input._1()._2();

        for (DBSCANCluster cluster : crowd) {
            List<TCPoint> points = cluster._cluster.getPoints();
            List<Tuple2<Integer,Integer>> pointList = new ArrayList();
            for (TCPoint point : points) {
                pointList.add(new Tuple2(
                        point.getObjectId(),
                        point.getTimeStamp()));
            }
            result.add(new Tuple2(crowdId, pointList));
        }
        return result;
    }
}
