package common.data;

import common.geometry.Coordinate2D;
import common.geometry.TCPoint;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.util.Precision;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KDTree {

    private int _maxNumOfLeaf;
    private KDNode _root;

    public KDTree(int maxNumOfLeaf) {
        _maxNumOfLeaf = maxNumOfLeaf;
    }

    public boolean isEmpty() {
        return _root == null;
    }

    public void buildTree(List<TCPoint> points)
    {
        _root = new KDNode();
        _root.addPoints(points);
        breathFirstBuildTree(points);
    }

    private void breathFirstBuildTree(List<TCPoint> points)
    {
        Queue<KDNode> queue = new LinkedList<KDNode>();
        queue.add(_root);

        while(getLeafNodeCount(_root) < _maxNumOfLeaf) {

            if(queue.isEmpty())
                return;

            KDNode node = queue.remove();
            List<TCPoint> curPoints = node.getPoints();

            if(curPoints.size() == 1)
                continue;

            // compute the variance in each dimension
            Variance variance = new Variance();
            Median median = new Median();
            Coordinate2D coordinate;
            double varX, varY, med;
            double[] xs = toArray(curPoints, Coordinate2D.XAxis);
            double[] ys = toArray(curPoints, Coordinate2D.YAxis);

            varX = variance.evaluate(xs);
            varY = variance.evaluate(ys);

            // pick up the middle value in the dimension with larger variance
            if (varX > varY) {
                coordinate = Coordinate2D.XAxis;
                med = median.evaluate(xs);
            } else {
                coordinate = Coordinate2D.YAxis;
                med = median.evaluate(ys);
            }

            List<TCPoint> less = new ArrayList<>();
            List<TCPoint> greater = new ArrayList<>();
            for (TCPoint p : curPoints) {
                if (coordinate == Coordinate2D.XAxis) {
                    // add to both lists if a point is near the border
                    if(p.getX() >= med - Precision.EPSILON && p.getX() <= med + Precision.EPSILON)
                    {
                        less.add(p);
                        greater.add(p);
                    }
                    else if (p.getX() < med - Precision.EPSILON)
                        less.add(p);
                    else
                        greater.add(p);
                } else {
                    if(p.getY() >= med - Precision.EPSILON &&
                            p.getY() <= med + Precision.EPSILON) {
                        less.add(p);
                        greater.add(p);
                    }
                    else if (p.getY() < med - Precision.EPSILON) {
                        less.add(p);
                    }
                    else {
                        greater.add(p);
                    }
                }
            }

            KDNode left = new KDNode();
            left.addPoints(less);
            queue.add(left);

            KDNode right = new KDNode();
            right.addPoints(greater);
            queue.add(right);

            node._splitValue = med;
            node._splitCoordinate = coordinate;
            node.setLeftNode(left);
            node.setRightNode(right);
        }
    }

    public List<KDNode> getAllLeafNodes()
    {
        return getAllLeafNodes(_root);
    }

    private List<KDNode> getAllLeafNodes(KDNode node)
    {
        List<KDNode> leafNodes = new ArrayList<KDNode>();
        if(node.isLeaf())
            leafNodes.add(node);
        else
        {
            leafNodes.addAll(getAllLeafNodes(node.getLeftNode()));
            leafNodes.addAll(getAllLeafNodes(node.getRightNode()));
        }
        return leafNodes;
    }

    private int getLeafNodeCount(KDNode node)
    {
        if(node == null)
            return 0;
        if(node.getLeftNode() == null && node.getRightNode() == null)
            return 1;

        return getLeafNodeCount(node.getLeftNode()) + getLeafNodeCount(node.getRightNode());
    }

    private double[] toArray(Iterable<TCPoint> points, Coordinate2D coord)
    {
        List<Double> values = new ArrayList<>();
        for(TCPoint p : points) {
            if(coord == Coordinate2D.XAxis)
                values.add(p.getX());
            else
                values.add(p.getY());
        }
        Double[] ds = values.toArray(new Double[values.size()]);
        return ArrayUtils.toPrimitive(ds);
    }
}
