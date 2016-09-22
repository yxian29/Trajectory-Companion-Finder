package tc;

import common.data.TCLine;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class LineToLineDistanceMapper implements
        PairFunction<Tuple2<String,Tuple2<Tuple2<Integer,TCLine>,Tuple2<Integer,TCLine>>>, String, Double> {

    @Override
    public Tuple2<String, Double> call(
            Tuple2<String, Tuple2<Tuple2<Integer, TCLine>, Tuple2<Integer, TCLine>>> input) throws Exception {

        int objectId1 = input._2._1._1;
        int objectId2 = input._2._2._1;
        TCLine line1 = input._2._1._2;
        TCLine line2 = input._2._2._2;

        double dist = line1.segDist(line2);

        return new Tuple2<>(
                String.format("%s,%s,%s", input._1, objectId1, objectId2)
                , dist);
    }
}
