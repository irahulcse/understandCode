package thesis.pet.repo;

import thesis.context.VehicleContext;
import thesis.pet.PETFragment;

public class NoPET implements PETFragment {

    @Override
    public VehicleContext execute(VehicleContext in) {
        in.setUpdateTimestamp(System.currentTimeMillis());
        if (in.getScalarData() != null) {
            in.getScalarData().setProcessBegin(System.currentTimeMillis());
            in.getScalarData().setProcessEnd(System.currentTimeMillis());
            in.getScalarData().setProcessedData(in.getScalarData().getData());
        }
        if (in.getLocationData() != null) {
            in.getLocationData().setProcessBegin(System.currentTimeMillis());
            in.getLocationData().setProcessEnd(System.currentTimeMillis());
            in.getLocationData().setProcessedData(in.getLocationData().getData());
        }
        if (in.getImageData() != null) {
            in.getImageData().setProcessBegin(System.currentTimeMillis());
            in.getImageData().setProcessEnd(System.currentTimeMillis());
            in.getImageData().setProcessedData(in.getImageData().getData());
        }
        return in;
    }
}
