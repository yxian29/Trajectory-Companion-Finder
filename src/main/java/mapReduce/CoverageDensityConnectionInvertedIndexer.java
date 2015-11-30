package mapReduce;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CoverageDensityConnectionInvertedIndexer implements
        PairFlatMapFunction<Tuple2<Integer,
                        List<Tuple2<Integer,Integer>>>,
                        String, Integer>, Serializable {
    @Override
    public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, List<Tuple2<Integer, Integer>>> t) throws Exception {

        List<Tuple2<String, Integer>> objPairList = new ArrayList<>();

        for(Tuple2<Integer,Integer> tuple : t._2())
        {
            String newKeyStr = String.format("(%s,%s)", tuple._1(), tuple._2());
            objPairList.add(new Tuple2<>(newKeyStr, t._1()));
        }

        return objPairList;
    }
}
