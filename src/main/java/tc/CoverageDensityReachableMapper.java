package tc;

import common.data.TCLine;
import common.data.TCPoint;
import common.data.TCPolyline;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.awt.geom.Line2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CoverageDensityReachableMapper
        implements PairFlatMapFunction<Tuple2<String,
        Tuple2<Tuple2<Integer,TCPoint>,Map<Integer,TCPolyline>>>,
        String, Integer> {

    private double _distanceThreshold = 0.0;

    public CoverageDensityReachableMapper(double distanceThreshold)
    {
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Iterable<Tuple2<String, Integer>> call(
            Tuple2<String, Tuple2<Tuple2<Integer, TCPoint>, Map<Integer, TCPolyline>>> input) throws Exception {

        Tuple2<Integer, TCPoint> pointTuple = input._2()._1();
        Map<Integer, TCPolyline> polylineMap = input._2()._2();
        String key = String.format("%s,%s", input._1(), pointTuple._1());

        List<Tuple2<String, Integer>> result = new ArrayList();

        for (Map.Entry<Integer, TCPolyline> entry: polylineMap.entrySet()) {

            TCPolyline polyline = entry.getValue();
            List<TCLine> lines = entry.getValue().getAsLineSegements();

            int pointObjId = pointTuple._2().getObjectId();
            int polylineObjId = polyline.getObjectId();

            if (pointObjId == polylineObjId)
                continue;

            for (TCLine line : lines) {
                Line2D line2D = new Line2D.Double();
                line2D.setLine(line.getPoint1().getX(), line.getPoint1().getY(),
                        line.getPoint2().getX(), line.getPoint2().getY());

                double dist = line2D.ptLineDist(pointTuple._2());

                if (dist <= _distanceThreshold) {
                    Tuple2<String, Integer> t1 = new Tuple2(
                            key,
                            polylineObjId);

                    if (!result.contains(t1))
                        result.add(t1);
                }
            }
        }
        return result;
    }
}
