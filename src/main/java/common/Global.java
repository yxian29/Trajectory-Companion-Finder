package common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class Global implements Serializable {

    // supper global variable to count batch jobs
    public static AtomicLong batchCount = new AtomicLong();
}
