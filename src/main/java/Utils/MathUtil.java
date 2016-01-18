package Utils;

import geometry.TCLine;
import geometry.TCPoint;

import java.util.List;

public class MathUtil {

    /**
     * given a line based on two points, and a point away from the line,
     * find the perpendicular distance from the point to the line.
     * see http://mathworld.wolfram.com/Point-LineDistance2-Dimensional.html
     * for explanation and definition.
     * @param point given point
     * @param line given line
     * @return distance between a point to line
     */
    public static double distance(TCPoint point, TCLine line) {
        TCPoint l1 = line.getPoint1();
        TCPoint l2 = line.getPoint2();

        return Math.abs((l2.getX() - l1.getX())*(l1.getY() - point.getY()) - (l1.getX() - point.getX())*(l2.getY() - l1.getY()))/
                Math.sqrt(Math.pow(l2.getX() - l1.getX(), 2) + Math.pow(l2.getY() - l1.getY(), 2));
    }

    /**
     * Calculate the maximum subarray in a given array
     * @param values an array of values
     * @return The number of subarray
     */
    public static int maxSubarray(List<Integer> values) {
        int maxLen = 1;
        for(int i = 0; i < values.size() - 1; i++)
        {
            // initialize min and max for all sub-arrays starting with i
            int min = values.get(i);
            int max = values.get(i);

            // consider all sub-arrays starting with i and ending with j
            for(int j = i + 1; j < values.size(); j++)
            {
                // update min and max in this sub-array if needed
                min = Math.min(min, values.get(j));
                max = Math.max(max, values.get(j));

                // If current sub-array has all contiguous elements
                if ((max - min) == j - i)
                    maxLen = Math.max(maxLen, max - min + 1);
            }
        }
        return maxLen;
    }

    /**
     * Calculate the maximum subarray in a given array using Kadane's algorithm
     * https://en.wikipedia.org/wiki/Maximum_subarray_problem
     * @param values an array of values
     * @return The number of subarray
     */
    public static int maxSubarray1(int[] values) {
        int max_end = 0;
        int max_cur = 0;
        for(int x : values) {
            max_end = Math.max(x, max_end + x);
            max_cur = Math.max(max_cur, max_end);
        }
        return max_cur;
    }

    /**
     * Calculate the variance of an array of values
     * @param values an array of values
     * @return The variance of the values
     **/
    public static double variance(double[] values) {
        double std = standardDeviation(values);
        return std * std;
    }

    /**
     * Calculate the standard deviation of an array of values
     * Standard deviation is a statistical measure of spread or variability. The
     * standard deviation is the root mean square (RMS) deviation of the values
     * from their arithmetic mean.
     * @param values
     * @return
     */
    public static strictfp double standardDeviation(double[] values) {
        double mean = mean(values);
        double dv = 0d;
        for (double d : values) {
            double dm = d - mean;
            dv += dm * dm;
        }
        return Math.sqrt(dv / (values.length - 1));
    }

    /**
     * Calculate the mean of an array of values
     * @param values an array of values
     * @return The mean of the values
     **/
    public static strictfp double mean(double[] values) {
        return sum(values) / values.length;
    }

    /**
     * Sum up all the values in an array
     * @param values an array of values
     * @return The sum of all values in the array
     **/
    public static strictfp double sum(double[] values) {
        if(values == null || values.length == 0) {
            throw new IllegalArgumentException();
        }

        double sum = 0;
        for(int i = 0; i < values.length; i++) {
            sum+= values[i];
        }
        return sum;
    }
}
