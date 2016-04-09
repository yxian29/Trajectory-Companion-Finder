package tc;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SlotRemoveSubPartitionIDMapper implements
        PairFunction<Tuple2<String,Iterable<Integer>>, Integer, Iterable<Integer>>
{
    @Override
    public Tuple2<Integer, Iterable<Integer>> call(
            Tuple2<String, Iterable<Integer>> input) throws Exception {
        String[] split = input._1().split(",");
        return new Tuple2(Integer.parseInt(split[0]), input._2());
    }
}
