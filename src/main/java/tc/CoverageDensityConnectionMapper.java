package tc;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class CoverageDensityConnectionMapper implements
        PairFunction<Tuple2<Integer,String>, String, Integer> {

    @Override
    public Tuple2<String, Integer> call(Tuple2<Integer, String> input) throws Exception {
        return new Tuple2(input._2(),input._1());
    }

}
