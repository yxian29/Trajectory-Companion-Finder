package geometry;

import java.awt.geom.Point2D;
import java.io.Serializable;

public class TCPoint extends Point2D implements Serializable{

    private int _objectId;
    private double _x;
    private double _y;
    private int _timestamp;

    public TCPoint(int id, double x, double y, int timestamp)
    {
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
    public double getY() {
        return _x;
    }

    @Override
    public void setLocation(double x, double y) {
        _x = x;
        _y = y;
    }
}
