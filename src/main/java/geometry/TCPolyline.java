package geometry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TCPolyline implements Serializable {

    private List<TCPoint> _points = new ArrayList<>();

    public List<TCPoint> getPoints()
    {
        return _points;
    }

    public void AddPoint(TCPoint point)
    {
        if(!_points.contains(point))
            _points.add(point);
    }

    public List<TCLine> getAsLines()
    {
        List<TCLine> lines = new ArrayList<>();
        _points.sort((o1, o2) -> o1.compareTo(o2) );
        for(int i = 0; i < _points.size() - 1; ++i)
        {
            TCLine line = new TCLine(_points.get(i),_points.get(i+1));
            lines.add(line);
        }
        return lines;
    }
}
