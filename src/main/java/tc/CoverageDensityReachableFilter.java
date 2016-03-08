package tc;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class CoverageDensityReachableFilter
    implements Function<Tuple2<String, Iterable<Integer>>, Boolean> {

    private int _densityThreshold = 0;

    public CoverageDensityReachableFilter(int densityThreshold)
    {
        _densityThreshold = densityThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Iterable<Integer>> v1) throws Exception {

        Iterable<Integer> densityReachableItr = v1._2();
        int size = IteratorUtils.toList(densityReachableItr.iterator()).size();
        return size >= _densityThreshold;
    }
}
