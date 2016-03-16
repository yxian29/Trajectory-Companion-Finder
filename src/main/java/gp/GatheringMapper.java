package gp;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class GatheringMapper implements
        PairFunction<Tuple2<Long, Tuple2<Iterable<Tuple2<Integer,Integer>>, Iterable<Integer>>>,
                Long, Tuple2<Integer, Iterable<Integer>>>
{
    @Override
    public Tuple2<Long, Tuple2<Integer, Iterable<Integer>>> call(
            Tuple2<Long, Tuple2<Iterable<Tuple2<Integer, Integer>>, Iterable<Integer>>> input) throws Exception {
        return apply(input);
    }

    private Tuple2<Long, Tuple2<Integer, Iterable<Integer>>> apply(
            Tuple2<Long, Tuple2<Iterable<Tuple2<Integer, Integer>>, Iterable<Integer>>> input) {
        long crowdId = input._1();
        Iterable<Tuple2<Integer, Integer>> objectIdTimestampPairs
                = input._2()._1();

        List<Integer> objectIds = new ArrayList();
        int timestamp = -1;

        for (Tuple2<Integer, Integer> pair : objectIdTimestampPairs) {
            if(timestamp == -1)
                timestamp = pair._2();

            objectIds.add(pair._1());
        }

        return new Tuple2(crowdId,
                new Tuple2(timestamp, objectIds));
    }
}
