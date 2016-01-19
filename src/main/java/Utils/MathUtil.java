package Utils;

import geometry.TCLine;
import geometry.TCPoint;

import java.util.Arrays;

public class MathUtil {

    /**
     * given a line based on two points, and a point away from the line,
     * find the perpendicular distance from the point to the line.
     * see http://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
     * for explanation and definition.
     * @param point given point
     * @param line given line
     * @return distance between a point to line
     */
    public static double distance(TCPoint point, TCLine line) {
        TCPoint p0 = point;
        TCPoint p1 = line.getPoint1();
        TCPoint p2 = line.getPoint2();

        return Math.abs(p1.getY() - p1.getY() * p0.getX() -
                (p2.getX() - p1.getX()) * p0.getY() + p2.getX()*p1.getY() - p2.getY()*p1.getX())/
                Math.sqrt(Math.pow(p2.getY() - p1.getY(), 2) + Math.pow(p2.getX() - p1.getX(), 2));
    }

    /**
     * Calculate the maximum subarray in a given array
     * @param values an array of values
     * @return The number of subarray
     */
    public static int maxSubarray(int[] values) {

        // make sure the array is sorted
        Arrays.sort(values);

        int maxLen = 1;
        for(int i = 0; i < values.length - 1; i++)
        {
            // initialize min and max for all sub-arrays starting with i
            int min = values[i];
            int max = values[i];

            // consider all sub-arrays starting with i and ending with j
            for(int j = i + 1; j < values.length; j++)
            {
                // update min and max in this sub-array if needed
                min = Math.min(min, values[j]);
                max = Math.max(max, values[j]);

                // If current sub-array has all contiguous elements
                if ((max - min) == j - i)
                    maxLen = Math.max(maxLen, max - min + 1);
            }
        }
        return maxLen;
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
