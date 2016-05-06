package common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class Global implements Serializable {

    // supper global variable to count batch jobs
    public static AtomicInteger batchCount = new AtomicInteger();
}
