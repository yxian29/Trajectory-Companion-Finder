package tc;

import common.utils.MathUtil;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class TrajectoryCompanionFilter implements Function<Tuple2<String, Iterable<Integer>>, Boolean> {

    private int _numContinousSlots = 0;

    public TrajectoryCompanionFilter(int numContinousSlots)
    {
        _numContinousSlots = numContinousSlots;
    }

    @Override
    public Boolean call(Tuple2<String, Iterable<Integer>> t) throws Exception {

        List<Integer> slotList = IteratorUtils.toList(t._2().iterator());
        int[] slotArray =  ArrayUtils.toPrimitive(slotList.toArray(new Integer[slotList.size()]));
        int result = MathUtil.maxSubarray(slotArray);
        return result >= _numContinousSlots;
    }

}