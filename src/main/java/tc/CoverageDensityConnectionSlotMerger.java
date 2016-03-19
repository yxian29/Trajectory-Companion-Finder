package tc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class CoverageDensityConnectionSlotMerger implements
        PairFunction<Tuple2<Integer,Iterable<Integer>>, Integer, Iterable<Integer>>
{
    private List<Tuple2<Integer, Iterable<Integer>>> _broadcast;

    public CoverageDensityConnectionSlotMerger(
            Broadcast<List<Tuple2<Integer, Iterable<Integer>>>> broadcast) {
        _broadcast = broadcast.getValue();
    }

    @Override
    public Tuple2<Integer, Iterable<Integer>> call(Tuple2<Integer, Iterable<Integer>> input) throws Exception {
        Tuple2<Integer, Iterable<Integer>> thisConnection = input;
        int thisId = input._1();

        Set<Integer> accum = new TreeSet();
        accum.addAll(IteratorUtils.toList(thisConnection._2().iterator()));

        Set<Integer> remaining = new TreeSet();

        for (Tuple2<Integer, Iterable<Integer>> conn: _broadcast) {
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
