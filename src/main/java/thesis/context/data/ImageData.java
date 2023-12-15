package thesis.context.data;

/**
 * A Class that represents the raw sensor reading of type byte array
 */
public class ImageData extends Data<byte[]> {

    private String sequenceNumber;

    public ImageData(byte[] byteArray) {
        super("image", byteArray,System.currentTimeMillis());
    }

    public ImageData(byte[] byteArray, long timestamp){
        super("image", byteArray,timestamp);
    }

    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "Image data with sequence number " + sequenceNumber + ", key: " + key;
    }

}
