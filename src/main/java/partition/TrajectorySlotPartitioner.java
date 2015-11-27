package partition;

import org.apache.spark.Partitioner;

public class TrajectorySlotPartitioner extends Partitioner {

    private int _numPartitions = 0;

    public TrajectorySlotPartitioner()
    {
    }

    public TrajectorySlotPartitioner(int numPartitions)
    {
        _numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return _numPartitions;
    }

    @Override
    public int getPartition(Object key) {
//        int timestamp = Integer.parseInt(key.toString());
        return 0;
    }
}
