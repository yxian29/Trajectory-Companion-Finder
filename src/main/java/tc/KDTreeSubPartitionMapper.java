package tc;

import common.data.KDNode;
import common.data.KDTree;
import common.data.TCPoint;
import common.data.TCRegion;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KDTreeSubPartitionMapper implements
        FlatMapFunction<Tuple2<Long,Iterable<TCPoint>>, Tuple2<Long, TCRegion>>,
        Serializable
{
    private int _numSubPartitions = 1;

    public KDTreeSubPartitionMapper(int numSubpartition)
    {
        _numSubPartitions = numSubpartition;
    }

    @Override
    public Iterable<Tuple2<Long, TCRegion>> call(Tuple2<Long, Iterable<TCPoint>> slot) throws Exception {

        List<Tuple2<Long, TCRegion>> regions = new ArrayList<>();
        long slotId = slot._1();
        KDTree kdTree = new KDTree(_numSubPartitions);
        kdTree.buildTree(IteratorUtils.toList(slot._2().iterator()));

        if(!kdTree.isEmpty())
        {
            int id = 1;
            List<KDNode> leafNodes = kdTree.getAllLeafNodes();
            for(KDNode node : leafNodes)
            {
                TCRegion region = new TCRegion(id, slotId);
                for (TCPoint point : node.getPoints())
                {
                    region.addPoint(point);
                }
                regions.add(new Tuple2<>(slotId, region));
                id++;
            }
        }

        return regions;
    }
}
