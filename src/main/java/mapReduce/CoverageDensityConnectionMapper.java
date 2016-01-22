package mapReduce;

import Utils.MathUtil;
import geometry.TCLine;
import geometry.TCPoint;
import geometry.TCPolyline;
import geometry.TCRegion;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CoverageDensityConnectionMapper implements
        PairFunction<Tuple2<Integer,TCRegion>,
                        Integer, List<Tuple2<Integer, Integer>>> {

    private double _distanceThreshold = 0.0;

    public CoverageDensityConnectionMapper(double distanceThreshold)
    {
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Tuple2<Integer, List<Tuple2<Integer, Integer>>> call(Tuple2<Integer, TCRegion> v1) throws Exception {

        List<Tuple2<Integer, Integer>> list = new ArrayList<>();

        TCRegion region = v1._2();
        Map<Integer, TCPoint> points = region.getPoints();
        Map<Integer, TCPolyline> polylines = region.getPolylines();

        for (Map.Entry<Integer, TCPoint> pointEntry : points.entrySet()) {
            for(Map.Entry<Integer, TCPolyline> polylineEntry : polylines.entrySet())
            {
                TCPoint point = pointEntry.getValue();
                TCPolyline polyline = polylineEntry.getValue();
                List<TCLine> lines = polylineEntry.getValue().getAsLineSegements();

                int pointObjId = point.getObjectId();
                int polylineObjId = polyline.getObjectId();

                if(pointObjId == polylineObjId)
                    continue;

                for(TCLine line : lines)
                {
                    double dist = MathUtil.distance(
                            point, line);

                    if(dist < _distanceThreshold)
                    {
                        if(pointObjId > polylineObjId)
                            list.add(new Tuple2<>(polylineObjId, pointObjId));
                        else
                            list.add(new Tuple2<>(pointObjId, polylineObjId));
                    }
                }
            }
        }

        return new Tuple2<>(v1._1(), list);
    }
}
