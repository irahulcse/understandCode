package thesis.pet.repo;

import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;
import thesis.pet.PETFragment;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class ImageAnonymizerPET implements PETFragment {

    private Process pythonProcess;
    private BufferedInputStream in;
    private BufferedOutputStream out;

    private BufferedReader err;

    public ImageAnonymizerPET(String pythonMainPath) throws IOException {
        try {
            pythonProcess = Runtime.getRuntime().exec("%s %s".formatted(GlobalConfig.imagePETRuntime, pythonMainPath));

            in = new BufferedInputStream(pythonProcess.getInputStream());
            out = new BufferedOutputStream(pythonProcess.getOutputStream());
            err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public VehicleContext execute(VehicleContext in) {
        if (!pythonProcess.isAlive()) {
            String line;
            try {
                while ((line = err.readLine()) != null) {
                    System.err.println(line);
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            throw new RuntimeException("Python process has been killed. Exit value: " + pythonProcess.exitValue());
        }
        ImageData imageData = in.getImageData();
        imageData.setProcessedData(process(imageData));
        imageData.setProcessTime(System.currentTimeMillis());
        in.update(imageData);
        return in;
    }


    public byte[] process(ImageData imageData) {
        byte[] bytes = imageData.getData();
        return this.generate(bytes);
    }

    public byte[] generate(byte[] bytes) {
        byte[] part1 = intToBytes(bytes.length);
        byte[] result = new byte[part1.length + bytes.length];
        ByteBuffer buffer = ByteBuffer.wrap(result);
        buffer.put(part1);
        buffer.put(bytes);
        result = buffer.array();
        try {
            //out.write(intToBytes(bytes.length));
            //out.write(bytes);
            out.write(result);
            out.flush();

            byte[] header = in.readNBytes(4); // might be < 4
            int fileLength = bytesToInt(header);

            return in.readNBytes(fileLength);

        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    private static byte[] intToBytes(int value) {
        return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
    }

    private static int bytesToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | ((bytes[3] & 0xFF));
    }

    private static BufferedImage imageFromBytes(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try {
            return ImageIO.read(bais);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A method for testing the functionality of the class. It's not used in the data pipeline.
     * Only the constructor of this class is called in the code of the data pipeline.
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        JFrame frame = new JFrame("Image");
        frame.setVisible(true);
        Box initContent = Box.createVerticalBox();
        initContent.add(new JLabel("Initialized successfully. Waiting for images."));
        frame.setContentPane(initContent);
        frame.pack();
        frame.setLocationRelativeTo(null);

        ImageAnonymizerPET ia = new ImageAnonymizerPET("/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/src/main/java/thesis/pet/repo/python/mode3.py");
        long b = 87;

        while (b < 206) {
            String name = b + ".png";
            File file = new File("/Users/zhudasch/JavaPlayground/ImageAnonymizer/pic/sequence/" + name);
            b += 1;

            BufferedImage image;
            byte[] fileContent = Files.readAllBytes(file.toPath());
            image = imageFromBytes(ia.generate(fileContent));

            File outputImage = new File("/Users/zhudasch/JavaPlayground/ImageAnonymizer/pic/process_example/" + name);
            ImageIO.write(image, "png",outputImage);

            Box content = Box.createVerticalBox();
            content.add(new JLabel(new ImageIcon(image)));

            frame.setContentPane(content);
            frame.pack();
            Thread.sleep(1000);
        }

    }
}
