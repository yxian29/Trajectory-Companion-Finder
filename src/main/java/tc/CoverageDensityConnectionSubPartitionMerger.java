package tc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class CoverageDensityConnectionSubPartitionMerger implements
        PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>
{
    private List<Tuple2<String, Iterable<Integer>>>
        _subpartBroadcast;

    public CoverageDensityConnectionSubPartitionMerger(
            Broadcast<List<Tuple2<String, Iterable<Integer>>>> subpartBroadcast) {
        _subpartBroadcast = subpartBroadcast.getValue();
    }

    @Override
    public Tuple2<String, Iterable<Integer>> call(final Tuple2<String, Iterable<Integer>> input) throws Exception {

        Tuple2<String, Iterable<Integer>> thisConnection = input;
        String thisId = input._1();

        Set<Integer> accum = new TreeSet();
        accum.addAll(IteratorUtils.toList(thisConnection._2().iterator()));

        Set<Integer> remaining = new TreeSet();

        for (Tuple2<String, Iterable<Integer>> conn: _subpartBroadcast) {
            if(conn._1().equals(thisId)) {

                Collection<Integer> connList = IteratorUtils.toList(conn._2().iterator());
                Collection<Integer> common = CollectionUtils.intersection(accum, connList);

                if (common.size() > 0)
                    accum.addAll(connList);
                else
                    remaining.addAll(connList);
            }
        }

        if(remaining.size() > 0) {
            Collection<Integer> common = CollectionUtils.intersection(accum, remaining);
            if(common.size() > 0)
                accum.addAll(remaining);
        }

        return new Tuple2(thisId, accum);
    }
}
