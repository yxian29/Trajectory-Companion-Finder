package tc;

import common.data.TCLine;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class LineSegmentsFilter implements
        Function<Tuple2<String, Tuple2<Tuple2<Integer, TCLine>, Tuple2<Integer, TCLine>>>, Boolean>{

    private static int _timeThreshold;

    public LineSegmentsFilter(int timeThreshold)
    {
        _timeThreshold = timeThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Tuple2<Tuple2<Integer, TCLine>, Tuple2<Integer, TCLine>>> input) throws Exception {

        // TODO: prune segement pair: dist_min(MBR1, MBR2) > epsilon
        //MBR mbr1 = new MBR();
        //MBR mbr2 = new MBR();

        int objectId1 = input._2._1._1;
        int objectId2 = input._2._2._1;
        TCLine line1 = input._2._1._2;
        TCLine line2 = input._2._2._2;

        if(objectId1 == objectId2)
            return false;

        if(Math.abs(line1.getTemporalPoint1().getTimeStamp()
                - line2.getTemporalPoint2().getTimeStamp()) > _timeThreshold ||
                Math.abs(line1.getTemporalPoint2().getTimeStamp()
                        - line2.getTemporalPoint1().getTimeStamp()) > _timeThreshold)
        {
            return false;
        }

        return true;
    }
}
