package TrajectoryCompanion;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;


public class CoverageDensityConnectionMapper implements
        PairFlatMapFunction<Tuple2<String,Iterable<Integer>>, String,Iterable<Integer>> {

    public CoverageDensityConnectionMapper()
    {
    }

    @Override
    public Iterable<Tuple2<String, Iterable<Integer>>> call(Tuple2<String, Iterable<Integer>> input) throws Exception {

        List<Tuple2<String, Iterable<Integer>>> list = new ArrayList<>();

        String[] split = input._1().split(",");
        int slotId = Integer.parseInt(split[0]);
        int objId = Integer.parseInt(split[2]);

        List<Integer> copy = new ArrayList<>();
        for (Integer i: input._2()) {
            copy.add(i);
        }
        copy.add(objId);

        // for each density reachable object, swap the object id as key
        for (Integer id : input._2()) {
            list.add(new Tuple2<String, Iterable<Integer>>(
                    String.format("%s,%s", slotId, id),
                    copy
            ));
        }

        return list;
    }

}
