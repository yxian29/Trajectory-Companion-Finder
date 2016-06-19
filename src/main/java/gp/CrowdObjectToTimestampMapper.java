package gp;

import common.data.Crowd;
import common.data.DBSCANCluster;
import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CrowdObjectToTimestampMapper implements
        PairFlatMapFunction<Tuple2<Tuple2<Integer, Crowd>, Long>,
                String, Integer>
{
    @Override
    public Iterable<Tuple2<String, Integer>> call(Tuple2<Tuple2<Integer, Crowd>, Long> input) throws Exception {
        return apply(input);
    }

    public Iterable<Tuple2<String, Integer>> apply(Tuple2<Tuple2<Integer, Crowd>, Long> input) {
        List<Tuple2<String, Integer>> result = new ArrayList();

        long crowdId = input._2();
        Crowd crowd = input._1()._2();

        for (DBSCANCluster cluster : crowd) {
            List<TCPoint> points = cluster._cluster.getPoints();
            for(TCPoint point : points) {
                result.add(new Tuple2(
                        String.format("%s-%s", crowdId, point.getObjectId()),
                        point.getTimeStamp()
                ));
            }
        }

        // <(crowdId, objectId), timestamp>
        return result;
    }
}
