package thesis.context.predicates;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import thesis.context.VehicleContext;

import javax.annotation.Nonnull;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <p>A class that summarizes the logical relation between multiple simple predicates.
 *  A complex predicate can be simple, meaning that it can consist only one simple predicate, such as one {@link ScalarPredicate} or one {@link LocationPredicate}.
 *  If multiple simple predicates are contained, they must be connected with a logical relation "AND" or "OR".
 *  A complex predicate can also be joined with another complex predicate with a logical relation.</p>
 * <p>Currently,
 *  this class only supports connecting the simple or complex predicates with a single kind of logic.
 *  Take the following complex for example. In this example, a condition is valid if the speed is between 25 and 50 or the location is on the campus: </p>
 *  <pre> v > 25 && v < 50 || (x,y) inside the campus</pre>
 *  This complex predicate should be constructed in this way:
 *  <ul>
 *      <li>Form a {@link ComplexPredicate} consisting {@code v>25} and {@code v<50} with the logic {@code AND }</li>
 *      <li>Join the result {@link ComplexPredicate} with a simple predicate {@code (x,y) IN campus} with the logic {@code OR}</li>
 *  </ul>
 */
public class ComplexPredicate {

    private enum Logic {AND, OR}

    private Logic logic = null;
    private final List<ScalarPredicate> scalarPredicates = new ArrayList<>();
    private final List<LocationPredicate> locationPredicates = new ArrayList<>();
    private final List<ComplexPredicate> combinedPredicates = new ArrayList<>();

    /**
     * Construct a complex predicate containing multiple simple predicates or complex predicates or a combination of them.
     * @param logic              logical "AND" and "OR"
     * @param scalarPredicates   predicate for scalar values, such as speed, time
     * @param locationPredicates predicate for location values of class {@link java.awt.geom.Point2D}
     * @param combinedPredicates other constructed predicates
     * @throws IllegalArgumentException if there are fewer than 2 predicates
     */
    public ComplexPredicate(@Nonnull String logic, Collection<ScalarPredicate> scalarPredicates, Collection<LocationPredicate> locationPredicates,
                            Collection<ComplexPredicate> combinedPredicates) throws IllegalArgumentException {
        if (logic.equalsIgnoreCase("AND") || logic.equalsIgnoreCase("OR")) {
            this.logic = Logic.valueOf(logic.toUpperCase());
        } else {
            throw new IllegalArgumentException("The logic should be either AND or OR");
        }

        if (scalarPredicates != null) {
            this.scalarPredicates.addAll(scalarPredicates);
        }
        if (locationPredicates != null) {
            this.locationPredicates.addAll(locationPredicates);
        }
        if (combinedPredicates != null) {
            this.combinedPredicates.addAll(combinedPredicates);
        }
        if (this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() < 2) {
            throw new IllegalArgumentException("There should be at least 2 predicates defined.");
        }
    }

    public ComplexPredicate(ScalarPredicate sp) {
        scalarPredicates.add(sp);
    }

    public ComplexPredicate(LocationPredicate lp) {
        locationPredicates.add(lp);
    }

    /**
     * Construct a complex predicate from a JSON string description.
     * @param json Description of the predicate in JSON format.
     */
    public ComplexPredicate(String json) {
        JSONObject o = new JSONObject(json);
        try {
            if (o.getString("logic") != null &&
                    (o.getString("logic").equalsIgnoreCase("AND") ||
                            o.getString("logic").equalsIgnoreCase("OR"))) {
                logic = Logic.valueOf(o.getString("logic"));
            }
        } catch (JSONException ignored) {
        }

        try {
            JSONArray scalarPredicateArray = o.getJSONArray("scalar");
            scalarPredicateArray.forEach(entry -> {
                this.scalarPredicates.add(new ScalarPredicate((JSONObject) entry));
            });
        } catch (JSONException ignored) {
        }

        try {
            JSONArray locationPredicateArray = o.getJSONArray("location");
            locationPredicateArray.forEach(entry -> {
                this.locationPredicates.add(new LocationPredicate((JSONObject) entry));
            });
        } catch (JSONException ignored) {
        }

        if ((this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() > 1) && this.logic == null) {
            throw new IllegalArgumentException("Only one predicate is allows since there's no logical relation defined.");
        }
        if ((this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() < 2) && this.logic != null) {
            throw new IllegalArgumentException("There should be at least 2 predicates defined with a logic of " + this.logic);
        }

    }

    public boolean evaluate(VehicleContext vc){
        if (vc == null) return false;
        if (logic == Logic.AND) {
            // if one of the predicate return false, then return false and break;
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = vc.scalarData.getData();
                    if (!sp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = vc.locationData.getData();
                    if (!lp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (!cp.evaluate(vc)) {
                        return false;
                    }
                    ;
                }
            }
            return true;
        }
        if (logic == Logic.OR) {
            // if one of the predicate returns true, then break and return true
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = vc.scalarData.getData();
                    if (sp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = vc.locationData.getData();
                    if (lp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (cp.evaluate(vc)) {
                        return true;
                    }
                }
            }
            return false;
        }
        if (logic == null){
            if (!scalarPredicates.isEmpty()){
                ScalarPredicate sp = scalarPredicates.get(0);
                Double d = vc.scalarData.getData();
                return sp.evaluate(d);
            }
            if (!locationPredicates.isEmpty()){
                LocationPredicate lp = locationPredicates.get(0);
                Point2D p = vc.locationData.getData();
                return lp.evaluate(p);
            }
            if (!combinedPredicates.isEmpty()){
                ComplexPredicate cp = combinedPredicates.get(0);
                return cp.evaluate(vc);
            }
        }
        throw new IllegalArgumentException("Unsupported logical operation: " + logic);
    }
}
