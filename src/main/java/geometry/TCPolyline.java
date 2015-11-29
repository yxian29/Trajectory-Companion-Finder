package geometry;

import java.io.Serializable;
import java.util.ArrayList;
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
}
