package mapReduce;

import Utils.MathUtil;
import com.google.common.primitives.Ints;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class TrajectoryCompanionFilter implements Function<Tuple2<String, Iterable<Integer>>, Boolean> {

    private int _numContinousSlots = 0;

    public TrajectoryCompanionFilter(int numContinousSlots)
    {
        _numContinousSlots = numContinousSlots;
    }

    @Override
    public Boolean call(Tuple2<String, Iterable<Integer>> t) throws Exception {

        int[] slotArray = Ints.toArray(IteratorUtils.toList(t._2().iterator()));
        int result = MathUtil.maxSubarray(slotArray);
        return result >= _numContinousSlots;
    }
}