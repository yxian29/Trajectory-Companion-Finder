package common.data;

import common.geometry.Coordinate2D;
import common.geometry.TCPoint;

import java.util.ArrayList;
import java.util.List;

public class KDNode {

    private List<TCPoint> _points = new ArrayList<>();
    private KDNode _left, _right;

    public KDNode()
    {
        _left = null;
        _right = null;
    }

    public KDNode getLeftNode() { return _left; }
    public void setLeftNode(KDNode ln) { _left = ln; }

    public KDNode getRightNode() { return _right; }
    public void setRightNode(KDNode rn) { _right = rn; }

    public double _splitValue;
    public Coordinate2D _splitCoordinate;

    public void addPoint(TCPoint p)
    {
        _points.add(p);
    }

    public void addPoints(List<TCPoint> points)
    {
        _points.addAll(points);
    }

    public List<TCPoint> getPoints() { return _points; }

    public boolean isLeaf()
    {
        return _left == null && _right == null;
    }
}
