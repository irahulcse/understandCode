package thesis.pet;

import thesis.context.VehicleContext;
import thesis.context.data.Data;

import java.util.List;
import java.util.Map;

/**
 * Every PET fragment should implement this interface. A stateful PET fragment should further implement
 * these methods: {@link #buildState(VehicleContext)}, {@link #buildState(Map.Entry)} and {@link #isReady()}.
 * The PET enforcement operator, where this fragment resides, will consult the {@link thesis.util.DBWrapper} to
 * get the necessary data records to build the internal state of the PET.
 */

public interface PETFragment {
    /**
     * For measurement purpose, do not generate a new instance of {@link VehicleContext}.
     * <p>
     * Correct approach: add the result of the corresponding data section(s) to the field {@link Data#processedData} and
     * attach the timestamp of processing to the field {@link Data#processTime}
     * @param in
     * @return
     */
    VehicleContext execute(VehicleContext in);

    /**
     * Warm up a stateful PET with one single aggregated sensor reading of class {@link VehicleContext}.
     * @param record
     */
    default void buildState(VehicleContext record) {
    }

    /**
     * Warm up a stateful PET with a list of data records of the class extending {@link Data}.
     * The key of the entry indicates the data section.
     * The value of the entry is the list of the data records.
     * A PET that needs input from multiple data sections can utilize this to identify the data records and build the state of this data section.
     * @param records
     */
    default void buildState(Map.Entry<String, List<? extends Data<?>>> records) {
    }

    /**
     * Method for the {@link StatefulSerialPETEnforcement} to check if the stateful PET is ready.
     * For stateless PET, this method returns {@code true} by default
     * @return
     */
    default boolean isReady(){
        return true;
    }

    // Note: warmUp method does not need to distinguish between time window and piecewise window.
    // If a PET can operate in two modes, the execute() method is implemented differently.
    // In this case, it can be actually split into two PETs.
}