package geometry;

import java.io.Serializable;
import java.util.*;

public class TCRegion implements Serializable {

    private int _rid;   //region id
    private int _sid;   //slot id

    private Map<Integer, TCPoint> _points = new TreeMap<>();

    private Map<Integer, TCPolyline> _polylines = new TreeMap<>();

    public TCRegion(int rid, int sid)
    {
        _rid = rid;
        _sid = sid;
    }

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
