package src.main.java;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.WritableUtils;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.NumberFormatException;

public class RCFileColumnKeyBufferGenerator {
    public static void main(String[] args) throws IOException {
        if (args == null || args.length < 1) {
            System.out.println("Usage: <RL file>");
            return;
        }

        DataOutputStream out = new DataOutputStream(new FileOutputStream(args[0] + ".enc"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        String readVal;
        try {
            while (true) {
                readVal = reader.readLine();
                long i = Long.parseLong(readVal);
                WritableUtils.writeVLong(out, i);
            }
        } catch (IOException | NumberFormatException e) {
            reader.close();
            out.close();
        }
    }
}
