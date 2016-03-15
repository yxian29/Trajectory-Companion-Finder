package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ClusterFilter implements
        Function<Tuple2<Tuple2<Integer, Cluster>, Tuple2<Integer, Cluster>>, Boolean> {

    private int _timeInterval; // delta-t
    private int _densityThrehold; // mu
    private double _distanceThreshold; // delta, Dist(C(ti),C(ti+1) <= delta

    public ClusterFilter(int timeInterval, int densityThrehold, double distanceThreshold) {
        _timeInterval = timeInterval;
        _densityThrehold = densityThrehold;
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Boolean call(Tuple2<Tuple2<Integer, Cluster>, Tuple2<Integer, Cluster>> input) throws Exception {
        return apply(input);
    }

    public boolean apply(Tuple2<Tuple2<Integer, Cluster>, Tuple2<Integer, Cluster>> clusterPair) {
        Tuple2<Integer, Cluster> cluster1 = clusterPair._1();
        Tuple2<Integer, Cluster> cluster2 = clusterPair._2();

        // same timestamp
        if(cluster1._1() == cluster2._1())
            return false;

        // density requirement
        if(validateClusterDensity(cluster1._2()) == false ||
                validateClusterDensity(cluster2._2()) == false)
            return false;

        // time interval requirement
        if(validateTimeIntervals(cluster1._2(), cluster2._2()) == false)
            return false;

        // distance requirement
        if(validateClusterDistance(cluster1._2(), cluster2._2()) == false)
            return false;

        return true;
    }

    private boolean validateClusterDensity(Cluster cluster) {
        // requirement: |C(ti)| >= mu
        return cluster.getPoints().size() >= _densityThrehold;
    }

    private boolean validateTimeIntervals(Cluster c1, Cluster c2) {

        TCPoint p1 = (TCPoint) c1.getPoints().get(0);
        TCPoint p2 = (TCPoint) c2.getPoints().get(0);

        // requirement: t(i+1) - t(i) <= delta-t
        return Math.abs(p1.getTimeStamp() - p2.getTimeStamp()) <= _timeInterval;
    }

    private boolean validateClusterDistance(Cluster c1, Cluster c2)
    {
        TCPoint p1 = (TCPoint)c1.getPoints().get(0);
        TCPoint p2 = (TCPoint)c2.getPoints().get(0);

        double dist = p1.distanceFrom(p2);

        return dist <= _distanceThreshold;
    }
}
