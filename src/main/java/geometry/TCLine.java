package geometry;

/**
 * Created by osboxes on 29/11/15.
 */
public class TCLine {

    private TCPoint _p1, _p2;

    public TCLine(TCPoint p1, TCPoint p2)
    {
        _p1 = p1;
        _p2 = p2;
    }

    public TCPoint getPoint1()
    {
        return _p1;
    }

    public TCPoint getPoint2()
    {
        return _p2;
    }

    public double getSlope()
    {
        return (_p2.getY() - _p1.getY()) / (_p2.getX() - _p1.getX());
    }

}
