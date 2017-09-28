package src.main.java;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import org.apache.hadoop.io.WritableUtils;
import java.io.IOException;

public class RCFileKeyBufferParser {
    public static void main(String[] args) throws IOException {
        if (args == null || args.length < 4) {
            System.out.println("Usage: <header data> <column num> <output folder> <data buffer base name>");
            return;
        }
        int columnNumber = Integer.parseInt(args[1]);
        DataInputStream in = new DataInputStream(new FileInputStream(args[0]));

        int numberRows = WritableUtils.readVInt(in);
        System.out.println(Integer.toString(numberRows));
        for (int i = 0; i < columnNumber; i++) {
            int colValLen = WritableUtils.readVInt(in);
            int colUncompValLen = WritableUtils.readVInt(in);
            int bufLen = WritableUtils.readVInt(in);
            byte data[] = new byte[bufLen];
            in.readFully(data, 0, bufLen);
            System.out.println(Integer.toString(colValLen) + " " + Integer.toString(colUncompValLen) + " " + Integer.toString(bufLen));
            int colIdx = i + 1;
            FileOutputStream fout = new FileOutputStream(args[2] + "/" + args[3] + Integer.toString(colIdx));
            fout.write(data);
            fout.close();
        }
        in.close();
    }
}
