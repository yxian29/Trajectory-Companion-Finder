package geometry;

import comparator.TCPointComparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TCPolyline implements Serializable {

    private int _objectId;
    private List<TCPoint> _points = new ArrayList<>();

    public TCPolyline(int objectId)
    {
        _objectId = objectId;
    }

    public int getObjectId() { return _objectId; }

    public List<TCPoint> getPoints()
    {
        return _points;
    }

    public void addPoint(TCPoint point)
    {
        if(!_points.contains(point))
            _points.add(point);
    }

    public List<TCLine> getAsLineSegements()
    {
        List<TCLine> lines = new ArrayList<>();
        Collections.sort(_points, new TCPointComparator());

        for(int i = 0; i < _points.size() - 1; ++i)
        {
            TCLine line = new TCLine(_points.get(i),_points.get(i+1));
            lines.add(line);
        }
        return lines;
    }
}
