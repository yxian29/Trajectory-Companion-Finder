package tc;

import common.data.TCPoint;
import common.data.TCRegion;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SubPartitionToPointsFlatMapper implements
        PairFlatMapFunction<Tuple2<Long,TCRegion>,
                String, Tuple2<Integer, TCPoint>>
{
    @Override
    public Iterable<Tuple2<String, Tuple2<Integer, TCPoint>>> call(Tuple2<Long, TCRegion> input) throws Exception {
        return apply(input);
    }

    public Iterable<Tuple2<String, Tuple2<Integer, TCPoint>>> apply(Tuple2<Long, TCRegion> input) {
        List<Tuple2<String, Tuple2<Integer, TCPoint>>> result =
                new ArrayList();

        TCRegion region = input._2();
        long slotId = input._1();
        int regionId = region.getRegionId();
        String key = String.format("%s,%s", slotId, regionId);
        Map<Integer, TCPoint> pointMap = region.getPoints();
        for (Map.Entry<Integer, TCPoint> entry: pointMap.entrySet()) {
            result.add(new Tuple2(
                    key,
                    new Tuple2(entry.getKey(), entry.getValue())
            ));
        }

        return result;
    }
}
