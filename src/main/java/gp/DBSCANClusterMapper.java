package gp;

import common.geometry.TCRegion;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class DBSCANClusterMapper
        implements PairFlatMapFunction<Tuple2<Integer, TCRegion>,
                String, Cluster> {

        private double _distanceThreshold = 0.0;
        private int _densityThreshold = 0;

        public DBSCANClusterMapper(double distanceThreshold, int densityThreshold)
        {
            _distanceThreshold = distanceThreshold;
            _densityThreshold = densityThreshold;
        }

        @Override
        public Iterable<Tuple2<String, Cluster>> call(Tuple2<Integer, TCRegion> input) throws Exception {

            String key = String.format("%s,%s", input._1(),
                    input._2().getRegionId());

            List<Tuple2<String, Cluster>> list = new ArrayList<>();

            TCRegion region = input._2();
            DBSCANClusterer dbscan = new DBSCANClusterer(_distanceThreshold, _densityThreshold);
            List<Cluster> clusters = dbscan.cluster(region.getPoints().values());
            for(Cluster cluster : clusters) {
                list.add(new Tuple2<>(key, cluster));
            }
            return list;
        }
}
