package tc;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class LineToLineDistanceFilter implements
        Function<Tuple2<String, Double>, Boolean> {

    private double _distanceThreshold;

    public LineToLineDistanceFilter(double distanceThreshold)
    {
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Double> input) throws Exception {
        return input._2 < _distanceThreshold;
    }
}
