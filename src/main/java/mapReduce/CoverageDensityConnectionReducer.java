package mapReduce;

import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CoverageDensityConnectionReducer implements
        Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>> {
    @Override
    public Iterable<Integer> call(Iterable<Integer> v1, Iterable<Integer> v2) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (Integer i1: v1) {
            if(!list.contains(i1))
                list.add(i1);
        }
        for (Integer i2: v2) {
            if(!list.contains(i2))
                list.add(i2);
        }
        Collections.sort(list);
        return list;
    }
}
