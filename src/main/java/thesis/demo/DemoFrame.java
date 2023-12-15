package thesis.demo;

import thesis.common.GlobalConfig;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoFrame extends JFrame {

    private final Map<String, JLabel> labelMap = new HashMap<>();
    private final int refreshFrequency;

    public DemoFrame(int refreshFrequency) throws HeadlessException, IOException {
        this.refreshFrequency = refreshFrequency;
        setTitle("Demo");
        setSize(3200, 1800);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        setLayout(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();

        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;

        gbc.ipadx = 130;
        gbc.ipady = 88;

        loadUISettingFromCSV();
    }

    private void addLabelAt(String label, int x, int y) {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = x;
        gbc.gridy = y;
        add(new JLabel(label), gbc);
    }

    private void addComponentAt(JComponent component, int x, int y) {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = x;
        gbc.gridy = y;
        add(component, gbc);
    }

    public void updateLocation(String name, String location) {
        JLabel label = labelMap.get(name);
        if (label == null) System.out.println("Cannot find the label '" + name + "'");
        else label.setText(location);
    }

    public void updateSpeed(String name, String speed) {
        JLabel label = labelMap.get(name);
        if (label == null) System.out.println("Cannot find the label '" + name + "'");
        else label.setText(speed);
    }

    public void updateImage(String name, ImageIcon image) {
        JLabel label = labelMap.get(name);
        if (label == null) System.out.println("Cannot find the label '" + name + "'");
        else label.setIcon(image);
    }

    public void updateImage(String name, String error){
        JLabel label = labelMap.get(name);
        if (label == null) System.out.println("Cannot find the label '" + name + "'");
        else label.setText(error);
    }

    public void syncUpdateImage(Map<String, ImageIcon> batch) throws InterruptedException {
        for( Map.Entry<String, ImageIcon> entry : batch.entrySet()){
            updateImage(entry.getKey(), entry.getValue());
        }
        Thread.sleep(1000L / refreshFrequency);
    }

    private void loadUISettingFromCSV() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(GlobalConfig.textInputSource+"/demoUIConfig.csv"));
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] parts = line.split(",");
            if (parts.length != 4) throw new RemoteException("Bad CSV format. The number of columns is not 4.");
            String type = parts[0].trim();
            String content = parts[1].trim();
            int x = Integer.parseInt(parts[2].trim());
            int y = Integer.parseInt(parts[3].trim());
            if (type.equals("label")) {
                addLabelAt(content, x, y);
            } else if (type.equals("comp")) {
                labelMap.put(content, new JLabel());
                addComponentAt(labelMap.get(content), x, y);
            } else throw new RuntimeException("Unknown type: " + type);
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            DemoFrame frame;
            try {
                frame = new DemoFrame(1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            frame.setVisible(true);
        });
    }
}
