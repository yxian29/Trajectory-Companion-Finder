package gp;

import common.geometry.TCPoint;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DBSCANClusterMapper
        implements PairFlatMapFunction<Tuple2<Integer, Iterable<TCPoint>>, Integer, Cluster> {

        private double _distanceThreshold = 0.0;
        private int _densityThreshold = 0;

        public DBSCANClusterMapper(double distanceThreshold, int densityThreshold)
        {
            _distanceThreshold = distanceThreshold;
            _densityThreshold = densityThreshold;
        }

        @Override
        public Iterable<Tuple2<Integer, Cluster>> call(Tuple2<Integer, Iterable<TCPoint>> input) throws Exception {

            List<Tuple2<Integer, Cluster>> list = new ArrayList<>();
            int key = input._1();
            Collection<TCPoint> points = IteratorUtils.toList(input._2().iterator());
            DBSCANClusterer dbscan = new DBSCANClusterer(_distanceThreshold, _densityThreshold);
            List<Cluster> clusters = dbscan.cluster(points);
            for(Cluster c : clusters) {
                list.add(new Tuple2<>(key, c));
                break;
            }

            return list;
        }
}
