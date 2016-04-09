package tc;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SubPartitionRemoveObjectIDMapper implements
        PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>
{
    @Override
    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> input) throws Exception {
        String[] split = input._1().split(",");
        List<Integer> objectIds = IteratorUtils.toList(input._2().iterator());
        objectIds.add(Integer.parseInt(split[1]));
        return new Tuple2(String.format("%s,%s", split[0], split[1]), objectIds);
    }
}
