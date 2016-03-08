package common.data;

import common.geometry.TCPoint;
import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class KDTreeTest extends TestCase {

    public void testBuildTree() throws Exception {

//        List<TCPoint> points = new ArrayList<>();
//
//        // object 1
//        points.add(new TCPoint(1, 4, 2, 0));
//        points.add(new TCPoint(1, 5, 8, 0));
//        // object 2
//        points.add(new TCPoint(2, 1, 4, 0));
//        // object 3
//        points.add(new TCPoint(3, 7, 9, 0));
//        points.add(new TCPoint(3, 10, 11, 0));
//
//        KDTree myTree = new KDTree(4);
//        myTree.buildTree(points);
//
//        Assert.assertEquals(false, myTree.isEmpty());
    }

    public void testGetAllLeafNodes() throws Exception {

        List<TCPoint> points = new ArrayList<>();

        // object 1
        points.add(new TCPoint(1, 4, 2, 0));
        points.add(new TCPoint(1, 5, 8, 0));
        // object 2
        points.add(new TCPoint(2, 1, 4, 0));
        // object 3
        points.add(new TCPoint(3, 7, 9, 0));
        points.add(new TCPoint(3, 10, 11, 0));

        KDTree myTree = new KDTree(4);
        myTree.buildTree(points);
        if(!myTree.isEmpty()) {
            List<KDNode> nodes = myTree.getAllLeafNodes();
            Assert.assertEquals(4, nodes.size());
        }
    }
}