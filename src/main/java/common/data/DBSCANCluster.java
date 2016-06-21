package common.data;
import org.apache.commons.math3.stat.clustering.Cluster;

import java.io.Serializable;
import java.util.List;

public class DBSCANCluster implements Serializable, Comparable<DBSCANCluster> {

    public Cluster _cluster;
    private int _timestamp;

    public DBSCANCluster(int timestamp, Cluster cluster)
    {
        _timestamp = timestamp;
        _cluster = cluster;
    }

    public Integer getTimeStamp()
    {
        return _timestamp;
    }

    public TCPoint centroid() {

        int size = _cluster.getPoints().size();
        if(size == 0)
            return new TCPoint(0,0,0,0);

        double centroidX = 0, centroidY = 0;
        List<TCPoint> knots = _cluster.getPoints();
        for (TCPoint knot: knots) {
            centroidX += knot.getX();
            centroidY += knot.getY();
        }
        return new TCPoint(0, centroidX / size, centroidY / size, 0);
    }

    @Override
    public int compareTo(DBSCANCluster o) {
        return this.getTimeStamp().compareTo(o.getTimeStamp());
    }
}
