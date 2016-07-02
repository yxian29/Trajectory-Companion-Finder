package gp;

import common.data.DBSCANCluster;
import common.data.TCPoint;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DBSCANClusterMapper
        implements PairFlatMapFunction<Tuple2<String, Iterable<TCPoint>>,
                   Integer, Tuple2<String, DBSCANCluster>> {

    private double _distanceThreshold = 0.0;
    private int _densityThreshold = 0;

    public DBSCANClusterMapper(double distanceThreshold, int densityThreshold) {
        _distanceThreshold = distanceThreshold;
        _densityThreshold = densityThreshold;
    }

    @Override
    public Iterable<Tuple2<Integer, Tuple2<String, DBSCANCluster>>>
    call(Tuple2<String, Iterable<TCPoint>> input) throws Exception {

        String[] split = input._1().split(",");
        String gridId = split[0];
        int timestamp = Integer.parseInt(split[1]);

        List<Tuple2<Integer, Tuple2<String, DBSCANCluster>>> result = new ArrayList<>();

        Collection<TCPoint> points = IteratorUtils.toList(input._2().iterator());
        DBSCANClusterer dbscan = new DBSCANClusterer(_distanceThreshold, _densityThreshold);
        List<Cluster> clusters = dbscan.cluster(points);
        for (Cluster c : clusters) {

            DBSCANCluster dbscanCluster = new DBSCANCluster(timestamp, c);
            Tuple2<String, DBSCANCluster> t = new Tuple2(gridId, dbscanCluster);

            result.add(new Tuple2(timestamp, t));
        }

        return result;
    }
}
