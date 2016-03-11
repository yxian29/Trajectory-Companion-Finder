package gp;

import common.geometry.TCPoint;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CrowdMapPartitionerTest extends TestCase {

    private int _timeInterval = 60; // delta-t
    private int _densityThreshold = 2; // mu
    private double _distanceThreshold = 0.005; // delta, Dist(C(ti),C(ti+1) <= delta

    public void testGetCrowds() throws Exception {

        List<Tuple2<Integer, Iterable<Cluster>>> input =
                new ArrayList<>();

        // initialize DBSCAN
        DBSCANClusterer dbscan = new DBSCANClusterer(_distanceThreshold, _densityThreshold);

        // cluster 1 at t = 100
        List<TCPoint> points1 = new ArrayList<>();
        points1.add(new TCPoint(1, 39.000134, 116.000909, 100));
        points1.add(new TCPoint(2, 39.000531, 116.000435, 100));
        points1.add(new TCPoint(3, 39.000234, 116.000459, 100));
        List<Cluster> cluster1 = dbscan.cluster(points1);

        // cluster 2 at t = 110
        List<TCPoint> points2 = new ArrayList<>();
        points2.add(new TCPoint(1, 39.000454, 116.000246, 110));
        points2.add(new TCPoint(3, 39.000564, 116.000896, 110));
        points2.add(new TCPoint(5, 39.000485, 116.000567, 110));
        List<Cluster> cluster2 = dbscan.cluster(points2);

        // cluster 2 at t = 190
        List<TCPoint> points3 = new ArrayList<>();
        points3.add(new TCPoint(1, 39.000154, 116.000246, 190));
        points3.add(new TCPoint(3, 39.000264, 116.000196, 190));
        points3.add(new TCPoint(5, 39.000285, 116.000267, 190));
        List<Cluster> cluster3 = dbscan.cluster(points3);

        // cluster 2 at t = 210
        List<TCPoint> points4 = new ArrayList<>();
        points4.add(new TCPoint(1, 39.000454, 116.000446, 210));
        points4.add(new TCPoint(3, 39.000464, 116.000496, 210));
        points4.add(new TCPoint(5, 39.000485, 116.000567, 210));
        List<Cluster> cluster4 = dbscan.cluster(points4);

        input.add(new Tuple2<Integer, Iterable<Cluster>>(100, cluster1));
        input.add(new Tuple2<Integer, Iterable<Cluster>>(110, cluster2));
        input.add(new Tuple2<Integer, Iterable<Cluster>>(190, cluster3));
        input.add(new Tuple2<Integer, Iterable<Cluster>>(210, cluster4));

        CrowdMapPartitioner instance = new CrowdMapPartitioner(
                _timeInterval, _densityThreshold, _distanceThreshold);

        Set<Tuple2<Integer, Iterable<Cluster>>> result =
                instance.getCrowds(input.iterator());

        Assert.assertEquals(2, result.size());
    }
}