package common.data;

import org.apache.commons.math3.stat.clustering.Clusterable;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.Collection;

public class TCPoint extends Point2D.Double implements Serializable, Clusterable<TCPoint>{

    private int _objectId;
    private int _timestamp;

    public TCPoint(int id, double xval, double yval, int timestamp)
    {
        super(xval,yval);

        _objectId = id;
        _timestamp = timestamp;
    }

    public int getObjectId() { return _objectId; }

    public int getTimeStamp() { return _timestamp; }

    public int compareTo(TCPoint comparePoint)
    {
        return this._timestamp - comparePoint.getTimeStamp();
    }

    @Override
    public double distanceFrom(TCPoint p) {
        return Math.sqrt(Math.pow(x - p.x, 2) + Math.pow(y - p.y, 2));
    }

    @Override
    public TCPoint centroidOf(Collection<TCPoint> collection) {
        return null;
    }
}
