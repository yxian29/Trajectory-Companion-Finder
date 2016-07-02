package gp;

import common.data.Crowd;
import common.data.TCPoint;
import org.apache.spark.api.java.function.Function;
import common.data.DBSCANCluster;

import java.util.*;

public class CrowdCandidatesMapper implements
        Function<Iterable<DBSCANCluster>,
                Iterable<Crowd>> {

    private int _timeInterval;
    private double _distanceThreshold;

    public CrowdCandidatesMapper(int timeInterval, double distanceThreshold)
    {
        _timeInterval = timeInterval;
        _distanceThreshold = distanceThreshold;
    }

    @Override
    public Iterable<Crowd> call(Iterable<DBSCANCluster> dbscanClusters) throws Exception {
        return getCrowds(dbscanClusters);
    }

    public Iterable<Crowd> getCrowds(Iterable<DBSCANCluster> clusters)
    {
        Set<Crowd> results = new HashSet<>(); // set of closed crowds
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
                    results.add(candidate);
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
            results.add(c);
        }

        return results;
    }

    private Map<Integer, Set<DBSCANCluster>> toTemporalOrderMap(Iterable<DBSCANCluster> clusters)
    {
        Map<Integer, Set<DBSCANCluster>> result = new TreeMap<>();
        for (DBSCANCluster cur: clusters)
        {
            if(result.containsKey(cur.getTimeStamp()))
                result.get(cur.getTimeStamp()).add(cur);
            else {
                Set<DBSCANCluster> cls = new HashSet<>();
                cls.add(cur);
                result.put(cur.getTimeStamp(), cls);
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
