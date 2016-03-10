package gp;

import common.geometry.TCPoint;
import common.geometry.TCRegion;
import org.apache.commons.math3.util.Precision;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SnapShotSubPartitionMapper implements
        FlatMapFunction<Tuple2<Integer,Iterable<TCPoint>>, Tuple2<Integer, TCRegion>>,
        Serializable {

    private static final double EPSILSON = Precision.EPSILON;
    private int _numSubPartitions = 1;

    public SnapShotSubPartitionMapper(int numSubPartitions)
    {
        _numSubPartitions = numSubPartitions;
    }

    @Override
    public Iterable<Tuple2<Integer, TCRegion>> call(Tuple2<Integer, Iterable<TCPoint>> input) throws Exception {
        List<Tuple2<Integer, TCRegion>> regions = new ArrayList<>();

        int timestamp = input._1();

        Tuple2<Double, Double> minMax = getMinMax(input._2());
        double min = minMax._1();
        double max = minMax._2();
        double length = (max - min) / _numSubPartitions;

        int key = timestamp;
        TCRegion value;
        for(int i = 1;i<= _numSubPartitions; ++i) {
            value = new TCRegion(i, input._1());
            Tuple2<Integer, TCRegion> t = new Tuple2<>(key, value);
            regions.add(t);
        }

        int id = 0;
        for(TCPoint point : input._2()) {
            // 0-base partition id
            id =  getSubPartitionId(point, min, length);
            if(id > 0) {
                Tuple2<Integer, TCRegion> r = regions.get(id - 1);
                r._2().addPoint(point);
            }

        }
        return regions;
    }

    private Tuple2<Double, Double> getMinMax(Iterable<TCPoint> points) {
        double max = 0.0;
        double min = 180.00;

        for(TCPoint point : points) {
            if(point.getY() > max)
                max = point.getY();

            if(point.getY() < min)
                min = point.getY();
        }

        return new Tuple2<>(min, max);
    }

    private int getSubPartitionId(TCPoint point, double min, double length) {
        double lowerBound = min;
        double upperBound = min + length + EPSILSON;
        for(int i = 1; i<= _numSubPartitions; ++i) {
            if(point.getY() > lowerBound && point.getY() <= upperBound)
                return i;

            lowerBound = upperBound - EPSILSON;
            upperBound = min + (i + 1) * length + EPSILSON;
        }
        return -1;
    }
}
