package mapReduce;

import geometry.TCPoint;
import geometry.TCRegion;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UniformSubPartitionMapper implements
        FlatMapFunction<Tuple2<Integer,Iterable<TCPoint>>, Tuple2<Integer, TCRegion>>,
        Serializable
{
    private int _numSubPartitions = 1;
    private double _epsilon = 0.0;

    public UniformSubPartitionMapper(int numSubpartition, double epsilon)
    {
        _numSubPartitions = numSubpartition;
        _epsilon = epsilon;
    }

    @Override
    public Iterable<Tuple2<Integer, TCRegion>> call(Tuple2<Integer, Iterable<TCPoint>> slot) throws Exception {

        List<Tuple2<Integer, TCRegion>> regions = new ArrayList<>();

        double max = getMaxY(slot._2());
        double min = getMinY(slot._2());
        double length = (max - min) / _numSubPartitions;

        for(int i = 1; i <= _numSubPartitions; ++i)
        {
            Tuple2<Integer, TCRegion> t = new Tuple2<>(slot._1(), new TCRegion(i, slot._1()));
            regions.add(t);
        }

        int id = 0;
        for (TCPoint point : slot._2())
        {
            id = getSubPartitionId(point, min, length);
            if(id > 0) {
                Tuple2<Integer, TCRegion> r = regions.get(id - 1);
                r._2().addPoint(point);
            }
        }

        return regions;
    }

    private double getMaxY(Iterable<TCPoint> points)
    {
        double max = 0.0;
        for (TCPoint point : points)
        {
            if (point.getY() > max)
                max = point.getY();
        }
        return max;
    }

    private double getMinY(Iterable<TCPoint> points)
    {
        double min = 180.0;
        for (TCPoint point : points)
        {
            if (point.getY() < min)
                min = point.getY();
        }
        return min;
    }

    private int getSubPartitionId(TCPoint point, double min, double length)
    {
        double lowerBound = min;
        double upperBound = min + length + _epsilon;
        for(int i = 1; i<= _numSubPartitions; ++i)
        {
            if(point.getY() >= lowerBound && point.getY() <= upperBound)
            {
                return i;
            }
            lowerBound = upperBound - _epsilon;
            upperBound = min + (i + 1) * length + _epsilon;
        }
        return -1;
    }


}