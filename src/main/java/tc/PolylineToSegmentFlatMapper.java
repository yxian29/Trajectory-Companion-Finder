package tc;

import common.data.TCLine;
import common.data.TCPolyline;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class PolylineToSegmentFlatMapper implements
        PairFlatMapFunction<Tuple2<String,Map<Integer,TCPolyline>>, String, Tuple2<Integer, TCLine>> {

    @Override
    public Iterable<Tuple2<String, Tuple2<Integer, TCLine>>> call(
            Tuple2<String, Map<Integer, TCPolyline>> input) throws Exception {

        String key = input._1();

        List<Tuple2<String, Tuple2<Integer, TCLine>>> result = new ArrayList();

        for (Map.Entry<Integer, TCPolyline> entry : input._2.entrySet()) {
            List<TCLine> lines = entry.getValue().getAsLineSegements();
            for (TCLine line: lines) {
                result.add(new Tuple2<String, Tuple2<Integer, TCLine>>(
                        key,
                        new Tuple2<Integer, TCLine>(entry.getKey(), line)
                ));
            }
        }

        return result;
    }
}
