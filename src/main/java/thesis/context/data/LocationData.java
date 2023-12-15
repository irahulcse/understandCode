package thesis.context.data;

import org.json.JSONObject;

import java.awt.geom.Point2D;

/**
 * A clas that represents the GPS readings. A GPS reading is a point of the class {@link Point2D}.
 * Besides, it has a field {@link #area} of type {@link String}. This field contains the name of the area.
 * For example, if (0,0) is defined to be the campus, then this field has the value "campus".
 */
public class LocationData extends Data<Point2D> {

    private String area;

    public LocationData(Point2D data) {
        super("location", data, System.currentTimeMillis());
    }

    public LocationData(Point2D data, long timestamp) {
        super("location", data, timestamp);
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getArea() {
        return this.area;
    }

    @Override
    public String toString() {
        if (processedData == null)
            return this.dataSection + ": (" + this.data.getX() + ", " + this.data.getY() + "), Timestamp: " + generateTime;
        return this.dataSection + ": (" + this.data.getX() + ", " + this.data.getY() + ") -> (" + this.processedData.getX()
                + ", " + this.data.getY() + ")";
    }

    /**
     * Intended for output the original coordinate in the demo
     * @return
     */
    public String originalDataToString() {
        Double x = data.getX();
        Double y = data.getY();
        return "[" + String.format("%.02f", x) + ", " + String.format("%.02f", y) + "]";
    }

    /**
     * Intended for output the processed coordinate in the demo
     * @return
     */
    public String processedDataToString(){
        if (processedData==null) return "-";
        Double x = processedData.getX();
        Double y = processedData.getY();
        return "[" + String.format("%.02f", x) + ", " + String.format("%.02f", y) + "]";
    }

}
