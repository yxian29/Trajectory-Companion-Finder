package gp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ParticipatorFilter implements
        Function<Tuple2<Long, Iterable<Integer>>, Boolean>
{
    private int _participatorNumThreshold = 0;

    public ParticipatorFilter(int participatorNumThreshold) {
        _participatorNumThreshold = participatorNumThreshold;
    }

    @Override
    public Boolean call(Tuple2<Long, Iterable<Integer>> input) throws Exception {
        return apply(input);
    }

    public Boolean apply(Tuple2<Long, Iterable<Integer>> input) {
        return IteratorUtils.toList(input._2().iterator()).size()
                >= _participatorNumThreshold;
    }
}
