package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ClusterFilter implements
        Function<Tuple2<Integer, Cluster>, Boolean> {
    private Cluster _thisCluster;
    private int _timeInterval; // delta-t

    public ClusterFilter(Cluster thisCluster, int timeInterval) {
        _thisCluster = thisCluster;
        _timeInterval = timeInterval;
    }

    @Override
    public Boolean call(Tuple2<Integer, Cluster> input) throws Exception {
        TCPoint point = (TCPoint) input._2().getPoints().get(0);
        TCPoint thisPoint = (TCPoint) _thisCluster.getPoints().get(0);
        int timestamp = point.getTimeStamp();
        int thisTimestamp = thisPoint.getTimeStamp();

        if(timestamp < thisTimestamp)
            return false;

        if(timestamp - thisTimestamp > _timeInterval)
            return false;

        return true;
    }

    private int getTimestampDiff(Cluster c1, Cluster c2) {
        TCPoint p1 = (TCPoint) c1.getPoints().get(0);
        TCPoint p2 = (TCPoint) c2.getPoints().get(0);

        int timestamp1 = p1.getTimeStamp();
        int timestamp2 = p2.getTimeStamp();

        return timestamp2 - timestamp1;
    }
}
