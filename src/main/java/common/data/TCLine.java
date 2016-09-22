package common.data;

import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.io.Serializable;

public class TCLine extends Line2D.Double implements Serializable {

    /* Reference
    http://code.metager.de/source/xref/lejos/classes/src_shared/lejos/geom/Line.java
     */

    public TCLine(TCPoint p1, TCPoint p2)
    {
        super(p1,p2);
    }

    /**
     * Calculate the point of intersection of two lines.
     *
     * @param l the second line
     *
     * @return the point of intersection or null if the lines do not intercept or are coincident
     */
    public Point2D.Double intersectsAt(Line2D.Double l) {
        double x, y, a1, a2, b1, b2;

        if (y2 == y1 && l.y2 == l.y1) return null; // horizontal parallel
        if (x2 == x1 && l.x2 == l.x1) return null; // vertical parallel

        // Find the point of intersection of the lines extended to infinity
        if (x1 == x2 && l.y1 == l.y2) { // perpendicular
            x = x1;
            y = l.y1;
        } else if (y1 == y2 && l.x1 == l.x2) { // perpendicular
            x = l.x1;
            y = y1;
        } else if (y2 == y1 || l.y2 == l.y1) { // one line is horizontal
            a1 = (y2 - y1) / (x2 - x1);
            b1 = y1 - a1 * x1;
            a2 = (l.y2 - l.y1) / (l.x2 - l.x1);
            b2 = l.y1 - a2 * l.x1;

            if (a1 == a2) return null; // parallel
            x = (b2 - b1) / (a1 - a2);
            y = a1 * x + b1;
        } else {
            a1 = (x2 - x1) / (y2 - y1);
            b1 = x1 - a1 * y1;
            a2 = (l.x2 - l.x1) / (l.y2 - l.y1);
            b2 = l.x1 - a2 * l.y1;

            if (a1 == a2) return null; // parallel
            y = (b2 - b1) / (a1 - a2);
            x = a1 * y + b1;
        }

        // Check that the point of intersection is within both line segments
        if (!between(x,x1,x2)) return null;
        if (!between(y,y1,y2)) return null;
        if (!between(x,l.x1,l.x2)) return null;
        if (!between(y,l.y1,l.y2)) return null;

        return new Point2D.Double(x, y);
    }

    /**
     * Return true iff x is between x1 and x2
     */
    private boolean between(double x, double x1, double x2) {
        if (x1 <= x2 && x >= x1 && x <= x2) return true;
        if (x2 < x1 && x >= x2 && x <= x1) return true;
        return false;
    }

    /**
     * Returns the minimum distance between two line segments--this line segment and another. If they intersect
     * the distance is 0. Lines can be parallel or skewed (non-parallel).
     * @param seg The other line segment.
     * @return The distance between the two line segments.
     */
    public double segDist(Line2D seg) {
        if(this.intersectsLine(seg))
            return 0;
        double a = Line2D.ptSegDist(this.getX1(), this.getY1(), this.getX2(), this.getY2(), seg.getX1(), seg.getY1());
        double b = Line2D.ptSegDist(this.getX1(), this.getY1(), this.getX2(), this.getY2(), seg.getX2(), seg.getY2());
        double c = Line2D.ptSegDist(seg.getX1(), seg.getY1(), seg.getX2(), seg.getY2(), this.getX1(), this.getY1());
        double d = Line2D.ptSegDist(seg.getX1(), seg.getY1(), seg.getX2(), seg.getY2(), this.getX2(), this.getY2());

        double minDist = a;
        minDist = (b<minDist?b:minDist);
        minDist = (c<minDist?c:minDist);
        minDist = (d<minDist?d:minDist);

        return minDist;
    }

    /**
     * Return the length of the line
     *
     * @return the length of the line
     */
    public float length() {
        return (float) Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
    }
}
