package thesis.context;

import org.apache.flink.api.java.tuple.Tuple2;
import thesis.common.Colors;
import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;
import thesis.demo.NewDemoSink;

import java.util.*;
import java.util.function.Supplier;

public class VehicleContext {

    public ScalarData scalarData;
    public LocationData locationData;
    public ImageData imageData;
    public long updateTimestamp; // This timestamp marks the latest change in each data section. If one section is updated, this value will be set to generation time of the updated section.
    private List<Tuple2<String, UUID>> triggers = new ArrayList<>(); // String is the affected data section
    private final transient Map<Class<?>, Supplier<Data<?>>> originalGetter = new HashMap<>();
    private int key;


    public VehicleContext() {
        //updateTimestamp = System.currentTimeMillis();
        supplierGetterForOriginalData();
    }

    public VehicleContext(VehicleContext other) {
        supplierGetterForOriginalData();
        if (other.scalarData != null) {
            this.scalarData = new ScalarData(other.scalarData.dataSection, other.scalarData.getData(), other.scalarData.getGenerateTime());
        }
        if (other.locationData != null) {
            this.locationData = new LocationData(other.locationData.getData(), other.locationData.getGenerateTime());
        }
        if (other.imageData != null) {
            this.imageData = new ImageData(other.imageData.getData(), other.imageData.getGenerateTime());
        }
        this.updateTimestamp = other.updateTimestamp;
        this.triggers = other.triggers;
        this.key = other.key;
    }

    public ScalarData getScalarData() {
        return scalarData;
    }

    public LocationData getLocationData() {
        return locationData;
    }


    public ImageData getImageData() {
        return imageData;
    }

    public int getKey() {
        return key;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setKey(int key) {
        this.key = key;
    }

    /**
     * Set the update time. This method will update the processed time to all included data sections.
     * It is intended for forwarding the data record by a PET that effectively do not process the record.
     *
     * @param updateTimestamp
     */
    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
        if (scalarData != null) {
            this.scalarData.setProcessTime(updateTimestamp);
        }
        if (locationData != null) {
            this.locationData.setProcessTime(updateTimestamp);
        }
        if (imageData != null) {
            this.imageData.setProcessTime(updateTimestamp);
        }
    }

    public void setScalarData(ScalarData scalarData) {
        this.scalarData = scalarData;
    }

    public void setLocationData(LocationData locationData) {
        this.locationData = locationData;
    }

    public void setImageData(ImageData imageData) {
        this.imageData = imageData;
    }

    public void update(ScalarData other) {
        if (scalarData == null || scalarData.getGenerateTime() < other.getGenerateTime()) {
            scalarData = other;
            if (updateTimestamp < other.getGenerateTime()) {
                updateTimestamp = other.getGenerateTime();
            }
        }
    }

    public void update(LocationData other) {
        if (locationData == null || locationData.getGenerateTime() < other.getGenerateTime()) {
            locationData = other;
            if (updateTimestamp < other.getGenerateTime()) {
                updateTimestamp = other.getGenerateTime();
            }
        }
    }

    public void update(ImageData other) {
        if (imageData == null || imageData.getGenerateTime() < other.getGenerateTime()) {
            imageData = other;
            if (updateTimestamp < other.getGenerateTime()) {
                updateTimestamp = other.getGenerateTime();
            }
        }
    }

    /**
     * Update a certain data section
     *
     * @param other
     */
    public void update(Data<?> other) {
        if (other instanceof ScalarData) {
            this.update((ScalarData) other);
        } else if (other instanceof LocationData) {
            this.update((LocationData) other);
        } else if (other instanceof ImageData) {
            this.update((ImageData) other);
        }
    }

    /**
     * Update each data section if this instance doesn't have it or the counterpart in the other instance is newer
     *
     * @param other another instance of {@link VehicleContext}
     */
    public void update(VehicleContext other) {
        // Update each data section if this instance doesn't have it or the counterpart in the other instance is newer
        if (other.scalarData != null && (this.scalarData == null || other.scalarData.getGenerateTime() > this.scalarData.getGenerateTime())) {
/*            if (this.scalarData != null) {
                System.out.println("Update Scalar data: " + this.scalarData.getData() + " -> " + other.scalarData.getData());
            } else {
                System.out.println("Update Scalar data: null -> " + other.scalarData.getGenerateTime());
            }*/
            this.scalarData = other.scalarData;
        }
        if (other.locationData != null && (this.locationData == null || other.locationData.getGenerateTime() > this.locationData.getGenerateTime())) {
/*            if (this.locationData != null) {
                System.out.println("Update location data: " + this.locationData.getData() + " -> " + other.locationData.getData());
            } else {
                System.out.println("Update location data: null -> " + other.locationData.getData());
            }*/
            this.locationData = other.locationData;
        }
        if (other.imageData != null && (this.imageData == null || other.imageData.getGenerateTime() > this.imageData.getGenerateTime())) {
/*            if (this.imageData != null) {
                System.out.println("Update image data: " + this.imageData.getSequenceNumber() + " -> " + other.imageData.getSequenceNumber());
            } else {
                System.out.println("Update image data: null -> " + other.imageData.getSequenceNumber());
            }*/
            this.imageData = other.imageData;
        }
        if (other.updateTimestamp > updateTimestamp) {
            // System.out.println("Update timestamp: " + updateTimestamp + " -> " + other.updateTimestamp);
            updateTimestamp = other.updateTimestamp;
        }
    }

    /**
     * Mark this VehicleContext as a trigger of a switch signal for one or multiple data sections (in case of PET concerning multiple data sections).
     *
     * @param dataSections A collection of the names of the data sections.
     */
    public void setTriggers(Collection<String> dataSections, UUID uuid) {

        for (String ds : dataSections) {
            triggers.add(Tuple2.of(ds, uuid));
        }
    }

    public boolean isTrigger() {
        return !triggers.isEmpty();
    }

    public <T extends Data<?>> T extractRecord(Class<T> recordClass) {
        Supplier<Data<?>> supplier = originalGetter.get(recordClass);
        if (supplier == null) throw new IllegalArgumentException("Unknown record type.");
        return recordClass.cast(supplier.get());
    }

    public static VehicleContext eraseProcessedData(VehicleContext vc) {
        VehicleContext out = new VehicleContext(vc);
        ScalarData scalarData = out.getScalarData();
        if (scalarData != null) {
            scalarData.setProcessedData(null);
            scalarData.setProcessTime(null);
            out.setScalarData(scalarData);
        }
        LocationData locationData = out.getLocationData();
        if (locationData != null) {
            locationData.setProcessedData(null);
            locationData.setProcessTime(null);
            out.setLocationData(locationData);
        }
        ImageData imageData = out.getImageData();
        if (imageData != null) {
            imageData.setProcessedData(null);
            imageData.setProcessTime(null);
            out.setImageData(imageData);
        }
        return out;
    }

    /**
     * Check if there exists a trigger for switching in the given dataSection.
     *
     * @param dataSection name of the dataSection
     * @return true if there exists a trigger for switching in the given dataSection.
     */
    public boolean isAffected(String dataSection) {
        if (triggers.isEmpty()) return false;
        else {
            for (Tuple2<String, UUID> tp : triggers) {
                if (tp.f0.equalsIgnoreCase(dataSection)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isAffected(Collection<String> dataSections) {
        if (triggers.isEmpty()) return false;
        else {
            for (Tuple2<String, UUID> tp : triggers) {
                if (!dataSections.contains(tp.f0)) {
                    return false;
                }
            }
            return true;
        }
    }

    public Set<UUID> getTriggerIDs() {
        Set<UUID> uuids = new HashSet<>();
        for (Tuple2<String, UUID> tp : triggers) {
            uuids.add(tp.f1);
        }
        return uuids;
    }

    public void setEvaluationTime(Long time) {
        if (scalarData != null) {
            this.scalarData.setEvaluationTime(time);
        }
        if (locationData != null) {
            this.locationData.setEvaluationTime(time);
        }
        if (imageData != null) {
            this.imageData.setEvaluationTime(time);
        }
    }

    @Override
    public String toString() {
        String records = "ScalarData: " + scalarData + " LocationData: " + locationData + " ImageData: " + imageData;
        StringBuilder triggers = new StringBuilder(Colors.ANSI_YELLOW + " Triggers: ");
        for (Tuple2<String, UUID> trigger : this.triggers) {
            triggers.append(trigger.toString());
        }
        triggers.append(Colors.ANSI_RESET);
        return records + triggers;
    }

    /**
     * Check if two VehicleContext have exactly the same data records (as for values and timestamps). Triggers are not considered.
     *
     * @param obj Comparison object.
     * @return true if the records are the same.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VehicleContext)) {
            return false;
        } else {
            return ((VehicleContext) obj).scalarData.equals(scalarData)
                    && ((VehicleContext) obj).locationData.equals(locationData);
            //&& ((VehicleContext) obj).imageData.equals(imageData);
        }
    }

    /**
     * Judge whether the other instance of {@link VehicleContext} has the same original values as this.
     * It is used in the {@link NewDemoSink} to synchronously refresh the processed contents in the frame that are queried by multiple applications.
     *
     * @param vc
     * @return
     */
    public boolean sameOrigin(VehicleContext vc) {
        if (getDataSectionAvailability() != vc.getDataSectionAvailability()) return false;
        int register = getDataSectionAvailability();
        if ((register & 1 << 2) == 1 << 2) {
            if (!this.scalarData.equals(vc.scalarData)) return false;
        }
        if ((register & 1 << 1) == 1 << 1) {
            if (!this.locationData.equals(vc.locationData)) return false;
        }
        if ((register & 1) == 1) {
            return this.imageData.equals(vc.imageData);
        }
        return true;
    }

    /**
     * 3-bit register to compare the availability.
     * From left to right, they are {@link ScalarData}, {@link LocationData} and {@link ImageData} respectively.
     * If a data section is not null, then its corresponding bit will be set to 1.
     *
     * @return
     */
    private int getDataSectionAvailability() {
        int register = 0;
        if (this.imageData != null) {
            register |= 1;
        }
        if (this.locationData != null) {
            register |= 1 << 1;
        }
        if (this.scalarData != null) {
            register |= 1 << 2;
        }
        return register;
    }

    public List<String> getAllDataSections() {
        List<String> dataSectionList = new ArrayList<>();
        dataSectionList.add("speed");
        dataSectionList.add("location");
        dataSectionList.add("image");
        return dataSectionList;
    }

    private void supplierGetterForOriginalData() {
        originalGetter.put(ScalarData.class, this::getScalarData);
        originalGetter.put(LocationData.class, this::getLocationData);
        originalGetter.put(ImageData.class, this::getImageData);
    }
}
