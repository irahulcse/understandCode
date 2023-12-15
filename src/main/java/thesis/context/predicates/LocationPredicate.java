package thesis.context.predicates;

import org.json.JSONArray;
import org.json.JSONObject;

import java.awt.geom.Path2D;
import java.awt.geom.Point2D;
import java.util.function.Predicate;

public class LocationPredicate {


    private enum Relation {
        IN, OUT_OF
    }

    private Predicate<Point2D> predicate;
    private String dataSection = "location";

    public LocationPredicate(Predicate<Point2D> predicate) {
        this.predicate = predicate;
    }

    public LocationPredicate(String string) {
        JSONObject o = new JSONObject(string);
        //this.dataSection = o.getString("data section");
        this.predicate = build(string);
    }

    public LocationPredicate(JSONObject o){
        String r = o.getString("relation");
        this.predicate = build(getArea(o), r);
    }

    public Path2D getArea(JSONObject o) {
        String r = o.getString("relation");
        JSONObject shape = o.getJSONObject("value");
        JSONArray x = shape.getJSONArray("x");
        JSONArray y = shape.getJSONArray("y");
        if (x.length() != y.length()) {
            throw new IllegalArgumentException("The number of xpoints and ypoints should be the same");
        }
        Path2D area = new Path2D.Double();
        area.moveTo(x.getDouble(0),y.getDouble(0));
        int length = x.length();
        for (int i = 1; i < length; i++) {
            area.lineTo(x.getDouble(i), y.getDouble(i));
        }
        area.closePath();
        return area;
    }

    public boolean evaluate(Point2D d) {
        return this.predicate.test(d);
    }

    public Predicate<Point2D> getPredicate() {
        return predicate;
    }

    public String getDataSection() {
        return dataSection;
    }

    public static Predicate<Point2D> build(Path2D area, String r) {
        return t -> {
            Relation relation = Relation.valueOf(r.toUpperCase());
            switch (relation) {
                case IN -> {
                    return area.contains(t);
                }
                case OUT_OF -> {
                    return !area.contains(t);
                }
            }
            throw new UnsupportedOperationException("Unsupported location relation: " + relation);
        };
    }

    public Predicate<Point2D> build(String string) {
        JSONObject o = new JSONObject(string);
        String r = o.getString("relation");
        Path2D area = getArea(o);
        return build(area, r);
    }
}
