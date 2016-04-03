package gp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import java.util.List;

public class CrowdTimeIntervalFilter implements
        Function<Iterable<Tuple2<Integer, Cluster>>, Boolean>
{
    private int _lifetimeThreshold;

    public CrowdTimeIntervalFilter(int lifetimeThreshold) {
        _lifetimeThreshold = lifetimeThreshold;
    }

    @Override
    public Boolean call(Iterable<Tuple2<Integer, Cluster>> input) throws Exception {
        List<Tuple2<Integer, Cluster>> clist = IteratorUtils.toList(input.iterator());

        if(clist.size() == 0)
            return false;

        if(clist.size() == 1)
            return true;

        Tuple2<Integer, Integer> minMax = getMinMax(clist);
        return minMax._2() - minMax._1() >= _lifetimeThreshold;
    }

    private Tuple2<Integer, Integer> getMinMax(List<Tuple2<Integer, Cluster>> list) {
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (Tuple2<Integer, Cluster> t: list) {
            if(t._1() > max)
                max = t._1();

            if(t._1() < min)
                min = t._1();
        }
        return new Tuple2(min, max);
    }
}
