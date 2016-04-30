package stc;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class InputDStreamValueMapper implements Function<Tuple2<String, String>, String> {
    @Override
    public String call(Tuple2<String, String> input) throws Exception {
        return input._2();
    }
}
