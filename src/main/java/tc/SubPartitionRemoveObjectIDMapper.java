package tc;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SubPartitionRemoveObjectIDMapper implements
        PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>
{
    @Override
    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> input) throws Exception {
        String[] split = input._1().split(",");
        return new Tuple2(String.format("%s,%s", split[0], split[1]), input._2());
    }
}
