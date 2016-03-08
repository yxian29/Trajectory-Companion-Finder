package common.utils;

import junit.framework.Assert;
import junit.framework.TestCase;

public class MathUtilTest extends TestCase {

    private int[] intArrayValues = new int[] { 5, 2, 4, 9, 6, 10 };

    public void testMaxSubarray() throws Exception {
        int max = MathUtil.maxSubarray(intArrayValues);
        Assert.assertEquals(3, max);
    }
}