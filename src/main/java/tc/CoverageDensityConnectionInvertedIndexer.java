package tc;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class CoverageDensityConnectionInvertedIndexer implements
        PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>, Serializable {
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> input) throws Exception {
        String t1 = input._2().toString();
        String[] split = input._1().split(",");
        int slotId = Integer.parseInt(split[0]);
        return new Tuple2<>(t1, slotId);
    }
}
