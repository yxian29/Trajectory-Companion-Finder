package stc;

import common.Global;
import common.data.TCPoint;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class TrajectorySlotMapper implements PairFunction<String, Long, TCPoint>, Serializable {
    @Override
    public Tuple2<Long, TCPoint> call(String line) throws Exception {
        String[] split = line.split(",");
        Integer objectId = Integer.parseInt(split[0]);
        Double x = Double.parseDouble(split[1]);
        Double y = Double.parseDouble(split[2]);
        Integer timestamp = 0;
        if(split[3].contains(":")) // assuming HH:mm format
        {
            timestamp = toHourMin(split[3]);
        }
        else // assuming integer format
        {
            timestamp = Integer.parseInt(split[3]);
        }
        TCPoint point = new TCPoint(objectId, x, y, timestamp);
        long slotId = Global.batchCount.get();

        return new Tuple2<>(slotId, point);
    }

    private int toHourMin(String timestamp)
    {
        if(!timestamp.contains(":"))
            return 0;

        String[] split = timestamp.split(":");
        int sec = Integer.parseInt(split[0]) * 60 + Integer.parseInt(split[1]);
        return sec;
    }
}