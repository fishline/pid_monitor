package src.main.java;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import org.apache.hadoop.io.WritableUtils;
import java.io.IOException;

public class RCFileColumnKeyBufferParser {
    public static void main(String[] args) throws IOException {
        if (args == null || args.length < 1) {
            System.out.println("Usage: <colKeyBufData>");
            return;
        }
        DataInputStream in = new DataInputStream(new FileInputStream(args[0]));

        int first = 1;
        try {
            while (true) {
                int num = WritableUtils.readVInt(in);
                if (first == 1) {
                    System.out.print(Integer.toString(num));
                } else {
                    System.out.print("," + Integer.toString(num));
                }
                first = 0;
            }
        } catch (IOException e) {
            System.out.print("\n");
        }
        in.close();
    }
}
