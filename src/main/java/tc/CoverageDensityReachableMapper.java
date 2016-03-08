package tc;

import common.geometry.TCLine;
import common.geometry.TCPoint;
import common.geometry.TCPolyline;
import common.geometry.TCRegion;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.awt.geom.Line2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CoverageDensityReachableMapper
    implements PairFlatMapFunction<Tuple2<Integer, TCRegion>,
                    String, Integer> {

    private double _distanceThreshold = 0.0;

    public CoverageDensityReachableMapper(double distanceThreshold)
    {
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, TCRegion> input) throws Exception {
        // input: (sid), Region}
        // output: (sid, rid, Ox), Oy

        List<Tuple2<String, Integer>> list = new ArrayList<>();
        TCRegion region = input._2();
        String key1, key2;

        Map<Integer, TCPoint> points = region.getPoints();
        Map<Integer, TCPolyline> polylines = region.getPolylines();

        for (Map.Entry<Integer, TCPoint> pointEntry : points.entrySet()) {
            for (Map.Entry<Integer, TCPolyline> polylineEntry : polylines.entrySet()) {
                TCPoint point = pointEntry.getValue();
                TCPolyline polyline = polylineEntry.getValue();
                List<TCLine> lines = polylineEntry.getValue().getAsLineSegements();

                int pointObjId = point.getObjectId();
                int polylineObjId = polyline.getObjectId();

                if (pointObjId == polylineObjId)
                    continue;

                for (TCLine line : lines) {
                    Line2D line2D = new Line2D.Double();
                    line2D.setLine(line.getPoint1().getX(), line.getPoint1().getY(),
                            line.getPoint2().getX(), line.getPoint2().getY());

                    double dist = line2D.ptLineDist(point);

                    if (dist <= _distanceThreshold) {

                        key1 = String.format("%s,%s,%s", input._1(), region.getRegionId(), polylineObjId);
                        key2 = String.format("%s,%s,%s", input._1(), region.getRegionId(), pointObjId);

                        Tuple2<String, Integer> t1 = new Tuple2<>(key1, pointObjId);
                        Tuple2<String, Integer> t2 = new Tuple2<>(key2, polylineObjId);
                        if (!list.contains(t1))
                            list.add(t1);

                        if (!list.contains(t2))
                            list.add(t2);
                    }
                }
            }
        }

        return list;
    }
}
