package gp;

import common.data.Crowd;
import common.data.TCPoint;
import org.apache.spark.api.java.function.Function2;
import common.data.DBSCANCluster;

import java.util.*;

import scala.Tuple2;

public class CrowdCandidatesMapper implements
        Function2<Integer, Iterator<Tuple2<Integer, DBSCANCluster>>,
                        Iterator<Tuple2<Integer,Crowd>>> {

    private int _timeInterval;
    private double _distanceThreshold;

    public CrowdCandidatesMapper(int timeInterval, double distanceThreshold)
    {
        _timeInterval = timeInterval;
        _distanceThreshold = distanceThreshold;
    }

    /**
     * Find crowd candidates in each temporal partitions
     * @param partitionId  current partition id
     * @param clusters     list of clusters within current partition
     * @return             list of crowd candidates
     */
    @Override
    public Iterator<Tuple2<Integer, Crowd>> call(Integer partitionId, Iterator<Tuple2<Integer, DBSCANCluster>> clusters) throws Exception {
        return getCrowds(partitionId, clusters);
    }

    public Iterator<Tuple2<Integer, Crowd>> getCrowds(Integer partitionId,
                                                      Iterator<Tuple2<Integer, DBSCANCluster>> clusters)
    {
        Set<Tuple2<Integer, Crowd>> results = new HashSet<>(); // set of closed crowds
        Set<Crowd> candidates = new HashSet<>(); // set of current crowd candidates

        Map<Integer, Set<DBSCANCluster>> timeOrderMap = toTemporalOrderMap(clusters);

        for (Map.Entry<Integer, Set<DBSCANCluster>> t: timeOrderMap.entrySet())
        {
            Crowd r = new Crowd();
            Set<Crowd> newCandidates = new HashSet<>();
            Set<Crowd> removedCandidates = new HashSet<>();

            for (Crowd candidate : candidates) {
                DBSCANCluster lastElement = candidate.last(); // the last snapshot cluster of cr
                Set<DBSCANCluster> cp = RangeSearch(lastElement, t.getValue());

                r.addAll(cp);
                if(cp.size() == 0) { // cr cannot be extended
                    results.add(new Tuple2<>(partitionId, candidate));
                }
                else
                {
                    for (DBSCANCluster cti : cp) {
                        Crowd crp = new Crowd();
                        crp.addAll(candidate);
                        crp.add(cti);
                        newCandidates.add(crp);
                    }
                }
                removedCandidates.add(candidate);
            }
            // sync candidates
            candidates.addAll(newCandidates);
            candidates.removeAll(removedCandidates);

            // the snapshot clusters that cannot be appended to any current
            // crowd will become new crowd candidates
            if(r.size() == 0) {
                for (DBSCANCluster cluster: t.getValue()) {
                    Crowd c = new Crowd();
                    c.add(cluster);
                    candidates.add(c);
                }
            }
        }

        for (Crowd c: candidates) {
            results.add(new Tuple2<>(partitionId, c));
        }

        return results.iterator();
    }

    private Map<Integer, Set<DBSCANCluster>> toTemporalOrderMap(Iterator<Tuple2<Integer, DBSCANCluster>> clusters)
    {
        Map<Integer, Set<DBSCANCluster>> result = new TreeMap<>();
        while(clusters.hasNext())
        {
            Tuple2<Integer, DBSCANCluster> cur = clusters.next();
            if(result.containsKey(cur._1()))
                result.get(cur._1()).add(cur._2());
            else {
                Set<DBSCANCluster> cls = new HashSet<>();
                cls.add(cur._2());
                result.put(cur._1(), cls);
            }
        }
        return result;
    }

    private Set<DBSCANCluster> RangeSearch(DBSCANCluster prev, Iterable<DBSCANCluster> cur)
    {
        Set<DBSCANCluster> result = new HashSet<>();
        for (DBSCANCluster c: cur) {
            if(validateClusterDistance(prev, c) &&
               validateTimeInterval(prev, c))
            {
                result.add(c);
            }
        }
        return result;
    }

    private boolean validateClusterDistance(DBSCANCluster c1, DBSCANCluster c2)
    {
        double dist = c1.centroid().distanceFrom(c2.centroid());
        return dist <= _distanceThreshold;
    }

    private boolean validateTimeInterval(DBSCANCluster c1, DBSCANCluster c2)
    {
        TCPoint p1 = (TCPoint)c1._cluster.getPoints().get(0);
        TCPoint p2 = (TCPoint)c2._cluster.getPoints().get(0);

        return Math.abs(p1.getTimeStamp() - p2.getTimeStamp()) <= _timeInterval;
    }
}
