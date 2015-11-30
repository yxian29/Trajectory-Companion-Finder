package Utils;

import geometry.TCLine;
import geometry.TCPoint;

import java.util.List;

public class MathUtil {

    public static class DistanceHelper {
        public static double distanceFromPointToLine(TCPoint point, TCLine line)
        {
            // given a line based on two points, and a point away from the line,
            // find the perpendicular distance from the point to the line.
            // see http://mathworld.wolfram.com/Point-LineDistance2-Dimensional.html
            // for explanation and definition.
            TCPoint l1 = line.getPoint1();
            TCPoint l2 = line.getPoint2();

            return Math.abs((l2.getX() - l1.getX())*(l1.getY() - point.getY()) - (l1.getX() - point.getX())*(l2.getY() - l1.getY()))/
                    Math.sqrt(Math.pow(l2.getX() - l1.getX(), 2) + Math.pow(l2.getY() - l1.getY(), 2));
        }
    }

    public static class MaxContiguousSubArrayFinder {
        public static int getMaxContiguousSubArray(List<Integer> list)
        {
            int maxLen = 1;
            for(int i = 0; i < list.size() - 1; i++)
            {
                // initialize min and max for all sub-arrays starting with i
                int min = list.get(i);
                int max = list.get(i);

                // consider all sub-arrays starting with i and ending with j
                for(int j = i + 1; j < list.size(); j++)
                {
                    // update min and max in this sub-array if needed
                    min = Math.min(min, list.get(j));
                    max = Math.max(max, list.get(j));

                    // If current sub-array has all contiguous elements
                    if ((max - min) == j - i)
                        maxLen = Math.max(maxLen, max - min + 1);
                }
            }
            return maxLen;
        }
    }
}
