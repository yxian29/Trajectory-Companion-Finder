package partition;

import geometry.TCLine;
import geometry.TCPoint;
import geometry.TCPolyline;
import geometry.TCRegion;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

import java.util.List;
import java.util.Map;


public class coverageDensityConnectionMapper implements Function<Tuple3<Integer,Integer,TCRegion>, Object> {

    private double _distanceThreshold = 0.0;

    public coverageDensityConnectionMapper(double distanceThreshold)
    {
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Tuple3<Integer, Integer, List<Integer>> call(Tuple3<Integer, Integer, TCRegion> v1) throws Exception {

        TCRegion region = v1._3();
        Map<Integer, TCPoint> points = region.getPoints();
        Map<Integer, TCPolyline> polylines = region.getPolylines();

        for (Map.Entry<Integer, TCPoint> pointEntry : points.entrySet()) {
            for(Map.Entry<Integer, TCPolyline> polylineEntry : polylines.entrySet())
            {
                List<TCLine> lines = polylineEntry.getValue().getAsLines();
                for(TCLine line : lines)
                {
                    // to-do
                }
            }
        }

        return null;
    }
}
