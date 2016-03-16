package gp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class GatheringFilter implements
        Function<Tuple2<Long, Tuple2<Iterable<Tuple2<Integer,Integer>>, Iterable<Integer>>>, Boolean>
{
    private int _participatorNumThreshold;

    public GatheringFilter(int participatorNumThreshold) {
        _participatorNumThreshold = participatorNumThreshold;
    }

    @Override
    public Boolean call(Tuple2<Long, Tuple2<Iterable<Tuple2<Integer, Integer>>, Iterable<Integer>>> input) throws Exception {
        return apply(input);
    }

    public Boolean apply(Tuple2<Long, Tuple2<Iterable<Tuple2<Integer, Integer>>, Iterable<Integer>>> input)  {
        Iterable<Tuple2<Integer,Integer>> cluster = input._2()._1();
        List<Integer> partitcipators = IteratorUtils.toList(input._2()._2().iterator());
        int count = 0;
        for (Tuple2<Integer, Integer> cid: cluster) {
            if(partitcipators.contains(cid._1()))
                count++;
        }
        return count >= _participatorNumThreshold;
    }

}
