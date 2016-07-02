package common.data;

import java.awt.geom.Point2D;
import java.io.Serializable;

public class MBR implements Serializable{

    private double _xmin;
    private double _xmax;
    private double _ymin;
    private double _ymax;

    public MBR(double xmin, double ymin, double xmax, double ymax)
    {
        _xmin = xmin;
        _xmax = xmax;
        _ymin = ymin;
        _ymax = ymax;
    }

    public MBR union(MBR b)
    {
        return new MBR(
                Math.min(this._xmin, b._xmin),
                Math.min(this._ymin, b._ymin),
                Math.max(this._xmax, b._xmax),
                Math.max(this._ymax, b._ymax)
        );
    }

    public boolean intersects(MBR b)
    {
        return !(
            this._xmin > b._xmax ||
               this._xmax < b._xmin ||
                    this._ymin > b._ymax ||
                    this._ymax < b._ymin
            );
    }

    public Point2D center()
    {
        return new Point2D.Double(
                (_xmin + _xmax) / 2,
                (_ymin + _ymax) / 2
        );
    }
}
