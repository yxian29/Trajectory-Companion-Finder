package common.data;

import org.apache.commons.math3.stat.clustering.Clusterable;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.Collection;

public class TCPoint extends Point2D implements Serializable, Clusterable<TCPoint>{

    private int _objectId;
    private double _x;
    private double _y;
    private int _timestamp;

    public TCPoint(int id, double x, double y, int timestamp)
    {
        _objectId = id;
        _x = x;
        _y = y;
        _timestamp = timestamp;
    }

    public int getObjectId() { return _objectId; }

    public int getTimeStamp() { return _timestamp; }

    @Override
    public double getX() {
        return _x;
    }

    @Override
    public double getY() { return _y; }

    @Override
    public void setLocation(double x, double y) {
        _x = x;
        _y = y;
    }


    public int compareTo(TCPoint comparePoint)
    {
        return this._timestamp - comparePoint.getTimeStamp();
    }

    @Override
    public double distanceFrom(TCPoint p) {
        return Math.sqrt(Math.pow(this._x - p._x, 2) + Math.pow(this._y - p._y, 2));
    }

    @Override
    public TCPoint centroidOf(Collection<TCPoint> collection) {
        return null;
    }
}
