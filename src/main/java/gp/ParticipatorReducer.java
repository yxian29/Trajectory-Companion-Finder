package gp;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class ParticipatorReducer implements
        Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>
{
    @Override
    public Iterable<Integer> call(Iterable<Integer> left, Iterable<Integer> right) throws Exception {
        return apply(left, right);
    }

    public Iterable<Integer> apply(Iterable<Integer> left, Iterable<Integer> right) {
        List<Integer> ls = IteratorUtils.toList(left.iterator());
        List<Integer> rs = IteratorUtils.toList(right.iterator());
        if (ls.retainAll(rs)) {
            ls.addAll(rs);
            List<Integer> unqiueSet = new ArrayList(
                    new LinkedHashSet(ls));

            return unqiueSet;
        }
        return new ArrayList();
    }
}
