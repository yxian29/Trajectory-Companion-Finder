package Utils;

import geometry.TCLine;
import geometry.TCPoint;
import junit.framework.Assert;
import junit.framework.TestCase;

public class MathUtilTest extends TestCase {

    private double[] doubleArrayValues = new double[] { 5.0, 2.0, 4.0, 9.0, 6.0, 10.0 };
    private int[] intArrayValues = new int[] { 5, 2, 4, 9, 6, 10 };

    public void testDistance() throws Exception {
        TCPoint p1 = new TCPoint(0, 0, 0, 0);
        TCPoint p2 = new TCPoint(0, 4, 4, 0);
        TCLine line = new TCLine(p1, p2);
        TCPoint p = new TCPoint(0, 4, 0, 0);
        double dist = MathUtil.distance(p, line);
        Assert.assertEquals(2.8284, dist, 0.0001);
    }

    public void testMaxSubarray() throws Exception {
        int max = MathUtil.maxSubarray(intArrayValues);
        Assert.assertEquals(3, max);
    }

    public void testVariance() throws Exception {
        double var = MathUtil.variance(doubleArrayValues);
        Assert.assertEquals(9.1999, var, 0.0001);
    }

    public void testStandardDeviation() throws Exception {
        double std = MathUtil.standardDeviation(doubleArrayValues);
        Assert.assertEquals(3.03315, std, 0.00001);
    }

    public void testMean() throws Exception {
        double mean = MathUtil.mean(doubleArrayValues);
        Assert.assertEquals(6, mean, 0.0);
    }

    public void testSum() throws Exception {
        double sum = MathUtil.sum(doubleArrayValues);
        Assert.assertEquals(36, sum, 0.0);
    }
}