package mapReduce;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CoverageDensityConnectionReducer implements Function2<
        List<Tuple2<Integer, Integer>>,
        List<Tuple2<Integer, Integer>>,
        List<Tuple2<Integer, Integer>>> {
    @Override
    public List<Tuple2<Integer, Integer>> call(List<Tuple2<Integer, Integer>> t1, List<Tuple2<Integer, Integer>> t2) throws Exception {
        t1.addAll(t2);
        return t1;
    }
}
