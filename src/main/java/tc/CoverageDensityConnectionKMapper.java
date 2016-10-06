package tc;


import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CoverageDensityConnectionKMapper implements
        PairFlatMapFunction<Tuple2<Integer,Iterable<Integer>>,
                Integer, Tuple2<Integer, Iterable<Integer>>> {

    @Override
    public Iterable<Tuple2<Integer, Tuple2<Integer, Iterable<Integer>>>> call(
            Tuple2<Integer, Iterable<Integer>> input) throws Exception {

        List<Tuple2<Integer, Tuple2<Integer, Iterable<Integer>>>> result =
                new ArrayList<>();

        int size = IteratorUtils.toList(input._2.iterator()).size();

        for(int i = 1; i <= size; i++)
        {
            if(i <= 10) {
                Tuple2<Integer, Iterable<Integer>> t = new Tuple2<>(i, input._2);
                result.add(new Tuple2<>(input._1, t));
            }
        }

        return result;
    }
}
