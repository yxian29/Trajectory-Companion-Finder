package sdp;

import org.apache.spark.api.java.function.Function;

public class TimestampFilter implements Function<String, Boolean> {

    private int _lowerBound ;
    private int _upperBound ;

    public TimestampFilter(int start, int end)
    {
        _lowerBound = start;
        _upperBound = end;
    }

    @Override
    public Boolean call(String line) throws  Exception {

        String[] split = line.split(",");
        Integer timestamp;

        if(split[3].contains(":")) // assuming HH:mm format
        {
            timestamp = toHourMin(split[3]);
        }
        else // assuming integer format
        {
            timestamp = Integer.parseInt(split[3]);
        }

        return (timestamp >= _lowerBound && timestamp < _upperBound);
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
