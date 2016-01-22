package data;

import geometry.Coordinate2D;
import geometry.TCPoint;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.descriptive.moment.Variance;
import org.apache.commons.math.stat.descriptive.rank.Median;

import java.util.ArrayList;
import java.util.List;

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
        buildTree(_root, points);
    }

    private void buildTree(KDNode node, List<TCPoint> points)
    {
        if(getLeafNodeCount(_root) > _maxNumOfLeaf)
            return;

        // compute the variance in each dimension
        Variance variance = new Variance();
        Median median = new Median();
        Coordinate2D coordinate;
        double varX, varY, med;
        double[] xs = toArray(points, Coordinate2D.XAxis);
        double[] ys = toArray(points, Coordinate2D.YAxis);

        varX = variance.evaluate(xs);
        varY = variance.evaluate(ys);

        // pick up the middle value in the dimension with larger variance
        if(varX > varY)
        {
            coordinate = Coordinate2D.XAxis;
            med = median.evaluate(xs);
        }
        else
        {
            coordinate = Coordinate2D.YAxis;
            med = median.evaluate(ys);
        }

        List<TCPoint> less = new ArrayList<>();
        List<TCPoint> greater = new ArrayList<>();
        for(TCPoint p : points)
        {
            if(coordinate == Coordinate2D.XAxis)
            {
                if(p.getX() < med)
                    less.add(p);
                else
                    greater.add(p);
            }
            else
            {
                if(p.getY() < med)
                    less.add(p);
                else
                    greater.add(p);
            }
        }

        KDNode left = new KDNode();
        left.addPoints(less);

        KDNode right = new KDNode();
        right.addPoints(greater);

        node._splitValue = med;
        node._splitCoordinate = coordinate;
        node.setLeftNode(left);
        node.setRightNode(right);

        // recursively build tree
        buildTree(left, less);
        buildTree(right, greater);
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
