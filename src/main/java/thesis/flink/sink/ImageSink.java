package thesis.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ImageSink extends RichSinkFunction<Tuple2<String, VehicleContext>> {
    private final JFrame frame = new JFrame("image");
    private int frequency;

    public ImageSink(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        frame.setVisible(true);
        Box initContent = Box.createVerticalBox();
        frame.setContentPane(initContent);
        frame.pack();
        frame.setLocationRelativeTo(null);
        System.out.println("ImageSink: frame is initialized.");
    }

    @Override
    public void invoke(Tuple2<String, VehicleContext> value, Context context) throws Exception {
        ImageData image = value.f1.getImageData();
        // System.out.println("ImageSink: received image: sequence" + image.getSequenceNumber() + " timestamp " + image.getGenerateTime() + " Process Latency: " + (image.getProcessTime() - image.getGenerateTime()) + " Sink Latency: " + (System.currentTimeMillis() - image.getProcessTime()));
        SwingUtilities.updateComponentTreeUI(frame);
        Box content = Box.createVerticalBox();
        ByteArrayInputStream bais = new ByteArrayInputStream(image.getProcessedData());
        try {
            BufferedImage bufferedImage = ImageIO.read(bais);
            content.add(new JLabel(new ImageIcon(bufferedImage)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        frame.setContentPane(content);
        frame.pack();
        Thread.sleep(1000 / frequency);
    }
}
