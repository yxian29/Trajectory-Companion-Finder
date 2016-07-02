package common.partition;
import common.data.TCPoint;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class FixedGridPartition implements GeospatialPartition<String,
        Integer, TCPoint>, Serializable {

    private double _gridSpacing;

    public FixedGridPartition(double gridSpacing)
    {
        if(gridSpacing <= 0.0)
            throw new IllegalArgumentException("Invalid grid spacing");

        _gridSpacing = gridSpacing;
    }

    @Override
    public JavaPairRDD<String, Iterable<TCPoint>> apply(JavaPairRDD<Integer, TCPoint> source) {

        return source.mapToPair(new PairFunction<Tuple2<Integer,TCPoint>,
                String, TCPoint>() {
            @Override
            public Tuple2<String, TCPoint> call(Tuple2<Integer, TCPoint> point) throws Exception {
                return new Tuple2<>(getGridId(point._2()), point._2());
            }
        }).groupByKey();
    }

    private String getGridId(TCPoint p) {

        int gridX = (int)Math.floor(p.getX() / _gridSpacing);
        int gridY = (int)Math.floor(p.getY() / _gridSpacing);
        return String.format("%s_%s,%s", gridX, gridY,p.getTimeStamp());
    }
}
