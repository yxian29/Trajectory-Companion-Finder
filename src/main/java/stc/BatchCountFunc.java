package stc;

import common.Global;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

public class BatchCountFunc implements Function2<JavaRDD<String>, Time, Void> {

    @Override
    public Void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
        if(stringJavaRDD.count() > 0) {
            if(Global.batchCount.get() == Long.MAX_VALUE) {
                // Opps, looks like a long run !! We need
                // to reset the counter.
                Global.batchCount.set(0);
            }
            else {
                Global.batchCount.getAndAdd(1);
            }
        }
        return null;
    }
}
