package thesis.context.predicates;

import org.json.JSONObject;

import java.util.Map;
import java.util.function.Predicate;

public class ScalarPredicate {


    private enum Relation {
        EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL
    }
    private final Predicate<Double> predicate;
    private final String dataSection;

    public ScalarPredicate(Predicate<Double> predicate, String dataSection) {
        this.predicate = predicate;
        this.dataSection = dataSection;
    }

    public ScalarPredicate(String string){
        JSONObject o = new JSONObject(string);
        this.dataSection = o.getString("data section");
        this.predicate = build(string);
    }

    public ScalarPredicate(JSONObject o){
        this.dataSection = o.getString("data section");
        String r = o.getString("relation");
        Double value = o.getDouble("value");
        this.predicate = build(r, value);
    }

    public Predicate<Double> getPredicate() {
        return predicate;
    }

    public String getDataSection() {
        return dataSection;
    }

    public boolean evaluate(Map<String, Object> contextMap){
        Double t = (Double) contextMap.get(this.dataSection);
        return evaluate(t);
    }

    public boolean evaluate(Double t){
        return this.predicate.test(t);
    }

    public Predicate<Double> build(String r, Double value) {
        return t -> {
            Relation relation = Relation.valueOf(r.toUpperCase());
            switch (relation) {
                case EQUAL -> {
                    return t.equals(value);
                }
                case GREATER -> {
                    return t.compareTo(value) > 0;
                }
                case LESS -> {
                    return t.compareTo(value) < 0;
                }
                case NOT_EQUAL -> {
                    return !t.equals(value);
                }
                case GREATER_EQUAL -> {
                    return t.compareTo(value) >= 0;
                }
                case LESS_EQUAL -> {
                    return t.compareTo(value) <= 0;
                }
            }
            throw new UnsupportedOperationException("Unsupported relation: " + relation);
        };
    }

    public Predicate<Double> build(String string){
        JSONObject o = new JSONObject(string);
        String r = o.getString("relation");
        Double value = o.getDouble("value");
        return build(r, value);
    }
}
