package tc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CoverageDensityConnectionSubsetKMapper implements
        PairFlatMapFunction<Tuple2<Integer,Tuple2<Integer,Iterable<Integer>>>,
                Integer, String> {

    public CoverageDensityConnectionSubsetKMapper()
    {

    }

    @Override
    public Iterable<Tuple2<Integer, String>> call(
            Tuple2<Integer, Tuple2<Integer, Iterable<Integer>>> input) throws Exception {

        List<Tuple2<Integer, String>> res = new ArrayList<>();

        List<Set<Integer>> subsets = getKLengthSubsets(
                IteratorUtils.toList(input._2._2.iterator()), input._2._1);

        for (Set<Integer> set: subsets) {
            res.add(new Tuple2<>(input._1, getStringFromItrable(set)));
        }

        return res;
    }

    public static List<Set<Integer>> getKLengthSubsets(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        getSubsets(superSet, k, 0, new HashSet<Integer>(), res);
        return res;
    }

    private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current,List<Set<Integer>> solution) {
        //successful stop clause
        if (current.size() == k) {
            solution.add(new HashSet<>(current));
            return;
        }
        //unseccessful stop clause
        if (idx == superSet.size()) return;
        Integer x = superSet.get(idx);
        current.add(x);
        //"guess" x is in the subset
        getSubsets(superSet, k, idx+1, current, solution);
        current.remove(x);
        //"guess" x is not in the subset
        getSubsets(superSet, k, idx+1, current, solution);
    }

    private String getStringFromItrable(Iterable<Integer> i) {
        return StringUtils.join(i.iterator(), ',');
    }
}
