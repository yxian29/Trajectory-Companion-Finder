package Utils;

import java.util.Arrays;

public class MathUtil {
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
}
