package tc;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class CoverageDensityConnectionSubsetMapper implements
        PairFlatMapFunction<Tuple2<Integer,Iterable<Integer>>,
                Integer, String>
{
    private int _sizeThreshold;

    public CoverageDensityConnectionSubsetMapper(int sizeThreshold) {
        _sizeThreshold = sizeThreshold;
    }

    @Override
    public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, Iterable<Integer>> input) throws Exception {

        List<Tuple2<Integer, String>> result = new ArrayList();
        Iterable<Set<Integer>> subsets = findSubsets(IteratorUtils.toList(input._2().iterator()));
        for (Set<Integer> set: subsets) {
            String subsetStr = getStringFromItrable(set);
            result.add(new Tuple2(input._1(), subsetStr));
        }
        return result;
    }

    private Iterable<Set<Integer>> findSubsets(List<Integer> input) {
        List<Set<Integer>> result = new ArrayList();

        int numOfSubsets = 1 << input.size();

        // TODO: remove this once we figure out how to calculate all subsets efficiently
        if(numOfSubsets > 10) // 2^10 subsets
            numOfSubsets = 10;

        for(int i=0; i< numOfSubsets; i++) {
            int pos = 0;
            int bitmask = i;

//            int bitCount = Integer.bitCount(i);
//            if(bitCount < _sizeThreshold)
//                continue;

            Set<Integer> set = new TreeSet();

            while(bitmask > 0) {
                if((bitmask & 1) == 1) {
                    set.add(input.get(pos));
                }

                bitmask >>= 1;
                pos++;
            }

            if(set.size() >= _sizeThreshold) {
                result.add(set);
            }
        }
        return result;
    }

    private String getStringFromItrable(Iterable<Integer> i) {
        return StringUtils.join(i.iterator(), ',');
    }
}
