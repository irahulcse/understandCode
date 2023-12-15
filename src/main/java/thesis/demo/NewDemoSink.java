package thesis.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import thesis.context.VehicleContext;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Receives unprocessed and processed data and present them in a JFrame.
 * The values are received in the form of a {@link Tuple2}. The String  at the first position indicates the label in the frame.
 * The payload has a generalized class of {@link Object}, since not only the {@link VehicleContext} but also the Strings indicating the
 * applied PET will be shown in the frame.
 */
public class NewDemoSink extends RichSinkFunction<Tuple2<String, VehicleContext>> {
    private final DemoFrame demoFrame;
    private final int refreshFrequency; // Set the refresh frequency, to display the figures in a more realistic way.
    private final int numberOfApp;
    private LinkedHashMap<String, Queue<VehicleContext>> appPresentationQueue = new LinkedHashMap<>();
    private List<VehicleContext> toPresent = new ArrayList<>();

    public NewDemoSink(DemoFrame demoFrame, int refreshFrequency, int numberOfApp) {
        this.demoFrame = demoFrame;
        this.refreshFrequency = refreshFrequency;
        this.numberOfApp = numberOfApp;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        demoFrame.setVisible(true);
    }

    @Override
    public void invoke(Tuple2<String, VehicleContext> value, Context context) throws Exception {
        String appName = value.f0;
        // System.out.println("Received from "+appName);
        if (!appPresentationQueue.containsKey(appName)) {
            appPresentationQueue.put(appName, new ArrayDeque<>());
            System.out.println("Added " + appName + " to the map.");
        }
        appPresentationQueue.get(appName).add(value.f1);

        handlePresentation();
    }

    private void present(VehicleContext vc, int index) throws InterruptedException {
        if (vc.scalarData != null) {
            if (index == 0) {
                demoFrame.updateSpeed("orig_speed", vc.scalarData.originalDataToString());
                demoFrame.updateSpeed("p_speed_" + (index+1), vc.scalarData.processedDataToString());
            } else {
                demoFrame.updateSpeed("p_speed_" + (index+1), vc.scalarData.processedDataToString());
            }
        }
        if (vc.locationData != null) {
            if (index == 0) {
                demoFrame.updateLocation("orig_loc", vc.locationData.originalDataToString());
                demoFrame.updateLocation("p_loc_" + (index+1), vc.locationData.processedDataToString());
            } else {
                demoFrame.updateLocation("p_loc_" + (index+1), vc.locationData.processedDataToString());
            }
        }
        if (vc.imageData != null) {
            if (index == 0) {
                try {
                    demoFrame.updateImage("orig_img", toImageIcon(vc.imageData.getData()));
                } catch (IOException e) {
                    demoFrame.updateImage("orig_img", "Unable to show the figure.");
                }
                try {
                    demoFrame.updateImage("p_img_" + (index+1), toImageIcon(vc.imageData.getProcessedData()));
                } catch (IOException e) {
                    demoFrame.updateImage("p_img_" + (index+1), "Unable to show the figure.");
                }
            } else {
                try {
                    demoFrame.updateImage("p_img_" + (index+1), toImageIcon(vc.imageData.getProcessedData()));
                } catch (IOException e) {
                    demoFrame.updateImage("p_img_" + (index+1), "Unable to show the figure.");
                }
            }
        }

    }


    private void handlePresentation() throws InterruptedException {
        // Not all the data have arrived.
        if (appPresentationQueue.size() != numberOfApp) return;

        for (Map.Entry<String, Queue<VehicleContext>> entry : appPresentationQueue.entrySet()) {
            // The next frame of a certain app is not arrived.
            if (entry.getValue().isEmpty()) {
                toPresent.clear();
                return;
            }
            toPresent.add(entry.getValue().peek());
        }
        // Following the break line above. If then next frame of certain apps are not available, clear the list for buffering.
//        if (toPresent.size() != numberOfApp) {
//            toPresent.clear();
//            return;
//        }
        for (int i = 0; i < toPresent.size(); i++) {
            present(toPresent.get(i), i);
        }
        clearPresentationBuffer();
        Thread.sleep(1000L / refreshFrequency);

    }

    private void clearPresentationBuffer() {
        toPresent.clear();
        for (Map.Entry<String, Queue<VehicleContext>> entry : appPresentationQueue.entrySet()) {
            entry.getValue().poll();
        }
    }

    private ImageIcon toImageIcon(byte[] imageAsByteArray) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(imageAsByteArray);
        BufferedImage bi = ImageIO.read(bais);
        return new ImageIcon(bi);
    }
}
