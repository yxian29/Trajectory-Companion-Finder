package common.data;

import java.io.Serializable;
import java.util.*;

public class TCRegion implements Serializable {

    private int _rid;   //region id
    private long _sid;   //slot id

    private Map<Integer, TCPoint> _points = new TreeMap<>();

    private Map<Integer, TCPolyline> _polylines = new TreeMap<>();

    public TCRegion(int rid, long sid)
    {
        _rid = rid;
        _sid = sid;
    }

    public long getSlotId() { return _sid; }

    public int getRegionId() { return _rid; }

    public void addPoint(TCPoint point) {
        _points.put(point.getObjectId(), point);

        if (_polylines.containsKey(point.getObjectId()))
        {
            _polylines.get(point.getObjectId()).addPoint(point);
        }
        else
        {
            TCPolyline pl = new TCPolyline(point.getObjectId());
            pl.addPoint(point);
            _polylines.put(point.getObjectId(), pl);
        }
    }

    public Map<Integer, TCPoint> getPoints()
    {
        return _points;
    }

    public Map<Integer, TCPolyline> getPolylines() { return _polylines; }
}
