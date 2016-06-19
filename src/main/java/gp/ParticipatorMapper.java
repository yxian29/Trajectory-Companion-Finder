package gp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class ParticipatorMapper implements
        PairFunction<Tuple2<String, Iterable<Integer>>, Long, Iterable<Integer>>
{
    @Override
    public Tuple2<Long, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> input) throws Exception {
        return apply(input);
    }

    public Tuple2<Long, Iterable<Integer>> apply(Tuple2<String, Iterable<Integer>> input) {
        String[] split = input._1().split("-");
        long crowdId = Integer.parseInt(split[0]);
        int objectId = Integer.parseInt(split[1]);
        List<Integer> participators = IteratorUtils.toList(input._2().iterator());
        participators.add(objectId);
        return new Tuple2(crowdId, participators);
    }
}
