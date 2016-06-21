package gp;

import common.data.Crowd;
import common.data.DBSCANCluster;
import common.data.TCPoint;
import junit.framework.TestCase;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CrowdCandidatesMapperTest extends TestCase {

    private int _timeInterval = 60; // delta-t
    private int _densityThreshold = 2; // mu
    private double _distanceThreshold = 0.005; // delta, Dist(C(ti),C(ti+1) <= delta

    public void testGetCrowds() throws Exception {

        List<Tuple2<Integer, DBSCANCluster>> input =
                new ArrayList<>();

        // initialize DBSCAN
        DBSCANClusterer dbscan = new DBSCANClusterer(_distanceThreshold, _densityThreshold);

        // cluster 1 at t = 100
        List<TCPoint> points1 = new ArrayList<>();
        points1.add(new TCPoint(1, 39.000134, 116.000909, 100));
        points1.add(new TCPoint(2, 39.000531, 116.000435, 100));
        points1.add(new TCPoint(3, 39.000234, 116.000459, 100));
        List<Cluster> c1 = dbscan.cluster(points1);
        for (Cluster c: c1) {
            input.add(new Tuple2<>(4, new DBSCANCluster(1, c)));
        }

        // cluster 2 at t = 110
        List<TCPoint> points2 = new ArrayList<>();
        points2.add(new TCPoint(1, 39.000454, 116.000246, 110));
        points2.add(new TCPoint(3, 39.000564, 116.000896, 110));
        points2.add(new TCPoint(5, 39.000485, 116.000567, 110));
        List<Cluster> c2 = dbscan.cluster(points2);
        for (Cluster c: c2) {
            input.add(new Tuple2<>(2, new DBSCANCluster(2, c)));
        }

        // cluster 2 at t = 190
        List<TCPoint> points3 = new ArrayList<>();
        points3.add(new TCPoint(1, 39.000154, 116.000246, 190));
        points3.add(new TCPoint(3, 39.000264, 116.000196, 190));
        points3.add(new TCPoint(5, 39.000285, 116.000267, 190));
        List<Cluster> c3 = dbscan.cluster(points3);
        for (Cluster c: c3) {
            input.add(new Tuple2<>(3, new DBSCANCluster(3, c)));
        }

        // cluster 2 at t = 210
        List<TCPoint> points4 = new ArrayList<>();
        points4.add(new TCPoint(1, 39.000454, 116.000446, 210));
        points4.add(new TCPoint(3, 39.000464, 116.000496, 210));
        points4.add(new TCPoint(5, 39.000485, 116.000567, 210));
        List<Cluster> c4 = dbscan.cluster(points4);
        for (Cluster c: c4) {
            input.add(new Tuple2<>(1, new DBSCANCluster(4, c)));
        }

        CrowdCandidatesMapper instance = new CrowdCandidatesMapper(_timeInterval, _distanceThreshold);

        Iterator<Tuple2<Integer, Crowd>> result = instance.getCrowds(1, input.iterator());

        assert(IteratorUtils.toList(result).size() == 4);
    }

}