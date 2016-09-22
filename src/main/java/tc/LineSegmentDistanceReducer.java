package tc;

import org.apache.spark.api.java.function.Function2;

public class LineSegmentDistanceReducer implements
        Function2<Double, Double, Double>{
    @Override
    public Double call(Double d1, Double d2) throws Exception {
        return Math.min(d1, d2);
    }
}
