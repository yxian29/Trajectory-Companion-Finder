package comparator;

import geometry.TCPoint;

import java.util.Comparator;

public class TCPointComparator implements Comparator<TCPoint> {
    public int compare(TCPoint p1, TCPoint p2)
    {
        return p1.compareTo(p2);
    }
}
