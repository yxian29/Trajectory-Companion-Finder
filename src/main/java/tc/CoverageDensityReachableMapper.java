package tc;

import com.google.common.base.Optional;
import common.geometry.TCLine;
import common.geometry.TCPoint;
import common.geometry.TCPolyline;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.awt.geom.Line2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CoverageDensityReachableMapper
        implements PairFlatMapFunction<Tuple2<String, Tuple2<Integer, TCPoint>>,
        String, Integer> {

    private Broadcast<List<Tuple2<String, Map<Integer, TCPolyline>>>>
            _broadcastPolylines;
    private double _distanceThreshold = 0.0;

    public CoverageDensityReachableMapper(double distanceThreshold,
                                          Broadcast<List<Tuple2<String, Map<Integer, TCPolyline>>>>
                                                  broadcastPolylineRDD)
    {
        _distanceThreshold = distanceThreshold;
        _broadcastPolylines = broadcastPolylineRDD;
    }

    @Override
    public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Tuple2<Integer, TCPoint>> input) throws Exception {

        List<Tuple2<String, Map<Integer, TCPolyline>>> allPolylines =
                _broadcastPolylines.getValue();

        String slotRegionKey = input._1();
        TCPoint point = input._2()._2();

        List<Tuple2<String, Integer>> result = new ArrayList();

        // the point object is reachable to itself
        result.add(new Tuple2(
                String.format("%s,%s", slotRegionKey, point.getObjectId()),
                point.getObjectId()
        ));

        for (Tuple2<String, Map<Integer, TCPolyline>> polylineTuple: allPolylines) {

            if(!polylineTuple._1().equals(slotRegionKey))
                continue;

            Map<Integer, TCPolyline> polylines = polylineTuple._2();
            for (Map.Entry<Integer, TCPolyline> polylineEntry : polylines.entrySet()) {

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
                        Tuple2<String, Integer> t1 = new Tuple2(
                                String.format("%s,%s", slotRegionKey, pointObjId),
                                polylineObjId);

                        if (!result.contains(t1))
                            result.add(t1);
                    }
                }
            }

        }


        return result;

    }

}
