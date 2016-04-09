package tc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function2;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class CoverageDensityConnectionReducer implements
        Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>
{

    @Override
    public Iterable<Integer> call(Iterable<Integer> iter1, Iterable<Integer> iter2) throws Exception {
        List<Integer> l1 = IteratorUtils.toList(iter1.iterator());
        List<Integer> l2 = IteratorUtils.toList(iter2.iterator());
        if(CollectionUtils.containsAny(l1,l2)) {
            Set<Integer> combined = new TreeSet(l1);
            for (Integer i: l2) {
                if(!combined.contains(i))
                    combined.add(i);
            }
            return combined;
        }
        return l2;
    }
}
