package tc;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class LineToDensityReachableMapper implements
        PairFunction<Tuple2<String,Double>, String, Integer> {
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Double> input) throws Exception {
        String[] splitStr = input._1.split(",");
        Integer slotId = Integer.parseInt(splitStr[0]);
        Integer regionId = Integer.parseInt(splitStr[1]);
        Integer objectId1 = Integer.parseInt(splitStr[2]);
        Integer objectId2 = Integer.parseInt(splitStr[3]);

        return new Tuple2<>(String.format("%s,%s,%s",
                slotId, regionId, objectId1), objectId2);
    }
}
