package topology;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.bytedeco.javacpp.opencv_core.CV_8UC1;
import static org.bytedeco.javacpp.opencv_core.cvMat;

/**
 * This class provides kryo serialization for the JavaCV's Mat and Rect objects, so that Storm can wrap them in tuples.
 * Serializable.Mat - kryo serializable analog of opencv_core.Mat object.<p>
 * Serializable.Rect - kryo serializable analog of opencv_core.Rect object.<p>
 * Serializable.PatchIdentifier is also kryo serializable object,
 * which is used to identify each patch of the frame.<p>
 * <p>
 *
 * @author Nurlan Kanapin
 * @see Serializable.Mat
 * @see Serializable.Rect
 * @see Serializable.PatchIdentifier
 */
public class Serializable {

    /**
     * Kryo Serializable Mat class.
     * Essential fields are image data itself, rows and columns count and type of the data.
     */
    public static class Mat implements KryoSerializable, java.io.Serializable {
        private byte[] data;
        private int rows, cols, type;

        public byte[] getData() {
            return data;
        }

        public int getRows() {
            return rows;
        }

        public int getCols() {
            return cols;
        }

        public int getType() {
            return type;
        }

        public Mat(){}

        /**
         * Creates new serializable Mat given its format and data.
         *
         * @param rows Number of rows in the Mat object
         * @param cols Number of columns in the Mat object
         * @param type OpenCV type of the data in the Mat object
         * @param data Byte data containing image.
         */
        public Mat(int rows, int cols, int type, byte[] data) {
            this.rows = rows;
            this.cols = cols;
            this.type = type;
            this.data = data;
        }

        /**
         * Creates new serializable Mat from opencv_core.Mat
         *
         * @param mat The opencv_core.Mat
         */
        public Mat(opencv_core.Mat mat) {
            if (!mat.isContinuous())
                mat = mat.clone();

            this.rows = mat.rows();
            this.cols = mat.cols();
            this.type = mat.type();
            int size = mat.arraySize();
            this.data = new byte[size];

            mat.getByteBuffer().get(this.data);
//            ByteBuffer bb = mat.getByteBuffer();
//            bb.rewind();
//            this.data = new byte[size];
//            while (bb.hasRemaining())  // should happen only once
//                bb.get(this.data);
        }

        /**
         * @return Converts this Serializable Mat into JavaCV's Mat
         */
        public opencv_core.Mat toJavaCVMat() {
            return new opencv_core.Mat(rows, cols, type, new BytePointer(data));
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(this.rows);
            output.writeInt(this.cols);
            output.writeInt(this.type);
            output.writeInt(this.data.length);
            output.writeBytes(this.data);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.rows = input.readInt();
            this.cols = input.readInt();
            this.type = input.readInt();
            int size = input.readInt();
            this.data = input.readBytes(size);
        }
    }

    /**
     * Kryo Serializable Rect class.
     */
    public static class Rect implements KryoSerializable, java.io.Serializable {
        /**
         * x, y, width, height - x and y coordinates of the left upper corner of the rectangle, its width and height
         */
        public int x, y, width, height;

        public Rect(){}

        public Rect(opencv_core.Rect rect) {
            x = rect.x();
            y = rect.y();
            width = rect.width();
            height = rect.height();
        }

        public Rect(int x, int y, int width, int height) {
            this.x = x;
            this.y = y;
            this.height = height;
            this.width = width;
        }

        public opencv_core.Rect toJavaCVRect() {
            return new opencv_core.Rect(x, y, width, height);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Rect rect = (Rect) o;

            if (height != rect.height) return false;
            if (width != rect.width) return false;
            if (x != rect.x) return false;
            if (y != rect.y) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = x;
            result = 31 * result + y;
            result = 31 * result + width;
            result = 31 * result + height;
            return result;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(x);
            output.writeInt(y);
            output.writeInt(width);
            output.writeInt(height);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            x = input.readInt();
            y = input.readInt();
            width = input.readInt();
            height = input.readInt();
        }
    }

    /**
     * This is a serializable class used for patch identification. Each patch needs to be distinguished form others.
     * Each patch is uniquely identified by the id of its frame and by the rectangle it corresponds to.
     */
    public static class PatchIdentifier implements KryoSerializable, java.io.Serializable {
        /**
         * Frame id of this patch
         */
        public int frameId;
        /**
         * Rectangle or Region of Interest of this patch.
         */
        public Rect roi;

        public PatchIdentifier(){}

        /**
         * Creates PatchIdentifier with given frame id and rectangle.
         *
         * @param frameId
         * @param roi
         */
        public PatchIdentifier(int frameId, Rect roi) {
            this.roi = roi;
            this.frameId = frameId;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(frameId);
            output.writeInt(roi.x);
            output.writeInt(roi.y);
            output.writeInt(roi.width);
            output.writeInt(roi.height);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            frameId = input.readInt();
            int x = input.readInt();
            int y = input.readInt();
            int width = input.readInt();
            int height = input.readInt();
            roi = new Rect(x, y, width, height);
        }

        /**
         * String representation of this patch identifier.
         *
         * @return the string in the format N%04d@%04d@%04d@%04d@%04d if roi is not null, and N%04d@null otherwise.
         */
        public String toString() {
            if (roi != null)
                return String.format("N%04d@%04d@%04d@%04d@%04d", frameId, roi.x, roi.y, roi.x + roi.width, roi.y + roi.height);
            return String.format("N%04d@null", frameId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PatchIdentifier that = (PatchIdentifier) o;

            if (frameId != that.frameId) return false;
            if (roi != null ? !roi.equals(that.roi) : that.roi != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = frameId;
            result = 31 * result + (roi != null ? roi.hashCode() : 0);
            return result;
        }
    }
}
