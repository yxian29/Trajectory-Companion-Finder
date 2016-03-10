package gp;

import common.geometry.TCPoint;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class CrowdMapPartitioner
            implements PairFlatMapFunction<Iterator<Tuple2<Integer,Iterable<Cluster>>>,
        Integer, Iterable<Cluster>> {

    private int _timeInterval; // delta-t
    private int _densityThrehold; // mu
    private double _distanceThreshold; // delta, Dist(C(ti),C(ti+1) <= delta

    public CrowdMapPartitioner(int timeInterval, int densityThrehold, double distanceThreshold)
    {
        _timeInterval = timeInterval;
        _densityThrehold = densityThrehold;
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Iterable<Tuple2<Integer, Iterable<Cluster>>> call(Iterator<Tuple2<Integer, Iterable<Cluster>>> input) throws Exception {

        Set<Tuple2<Integer, Iterable<Cluster>>> crowds = getCrowds(input);
        return crowds;
    }

    public Set<Tuple2<Integer, Iterable<Cluster>>> getCrowds(Iterator<Tuple2<Integer, Iterable<Cluster>>> clusters) {

        Tuple2<Integer, Iterable<Cluster>> cur = null;
        Tuple2<Integer, Iterable<Cluster>> prev = null;

        int cid = 1;
        Set<Tuple2<Integer, Iterable<Cluster>>> crowds = new HashSet<>();
        Set<Cluster> res = new HashSet<>();

        while(clusters.hasNext()) {

            if(prev == null) {
                prev = clusters.next();
                continue;
            }

            cur = clusters.next();

            for (Cluster c: prev._2()) {
                Set<Cluster> cp = RangeSearch(c, cur._2());
                res.addAll(cp);

                if(cp.size() == 0) {// Cr cannot be extended
                    if(validateTimeIntervals(prev._1(), cur._1())) {
                        crowds.add(new Tuple2<Integer, Iterable<Cluster>>(cid++, res));
                        res.clear();
                    }
                }
            }

            // update prev
            prev = cur;
        }

        return crowds;
    }

    private Set<Cluster> RangeSearch(Cluster prev, Iterable<Cluster> cur) {

        Set<Cluster> result = new HashSet<>();

        for (Cluster c: cur) {

            if(!validateClusterDensity(c))
                continue;

            if(validateClusterDistance(prev, c, _distanceThreshold))
            {
                result.add(c);
            }
        }

        return result;
    }

    private boolean validateClusterDensity(Cluster cluster) {
        // requirement: |C(ti)| >= mu
        return cluster.getPoints().size() >= _densityThrehold;
    }

    private boolean validateTimeIntervals(int t1, int t2) {
        // requirement: t(i+1) - t(i) <= delta-t
        return t1 - t2 <= _timeInterval;
    }

    private boolean validateClusterDistance(Cluster c1, Cluster c2, double delta)
    {
        TCPoint p1 = (TCPoint)c1.getPoints().get(0);
        TCPoint p2 = (TCPoint)c2.getPoints().get(0);

        double dist = p1.distanceFrom(p2);

        return dist <= delta;
    }
}
