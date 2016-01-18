package mapReduce;

import geometry.TCPoint;
import geometry.TCRegion;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KDTreeSubPartitionMapper implements
        FlatMapFunction<Tuple2<Integer,Iterable<TCPoint>>, Tuple3<Integer, Integer, TCRegion>>,
        Serializable
{
    private int _numSubPartitions = 1;
    private double _epsilon = 0.0;

    public KDTreeSubPartitionMapper(int numSubpartition, double epsilon)
    {
        _numSubPartitions = numSubpartition;
        _epsilon = epsilon;
    }

    @Override
    public Iterable<Tuple3<Integer, Integer, TCRegion>> call(Tuple2<Integer, Iterable<TCPoint>> slot) throws Exception {

        List<Tuple3<Integer, Integer, TCRegion>> regions = new ArrayList<>();

        int m = 1;
        while(m < _numSubPartitions)
        {
            m += 1;
        }


        return regions;
    }
}
