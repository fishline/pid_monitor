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
import java.io.File;

public class RCFileKeyBufferGenerator {
    public static void main(String[] args) throws IOException {
        if (args == null || args.length < 1) {
            System.out.println("Usage: <top folder of headers>");
            return;
        }
        File reportFile = new File(args[0] + "/logs/report.log");
        if (! reportFile.exists()) {
            System.out.println(args[0] + "/logs/report.log" + " does not exist, please run rcf.pl first!");
            return;
        }
        BufferedReader report = new BufferedReader(new InputStreamReader(new FileInputStream(args[0] + "/logs/report.log")));
        DataOutputStream out = new DataOutputStream(new FileOutputStream(args[0] + "/header.fix"));
        long l = 0L;
        try {
            String row_line = report.readLine();
            String[] row_line_parts = row_line.split(" ");
            l = Long.parseLong(row_line_parts[1]);
            WritableUtils.writeVLong(out, l);
            while (true) {
                String column_line = report.readLine();
                if ((column_line == null) || (!column_line.contains("column"))) {
                    break;
                }
                String[] column_line_parts = column_line.split(" ");
                l = Long.parseLong(column_line_parts[2]);
                WritableUtils.writeVLong(out, l);
                l = Long.parseLong(column_line_parts[3]);
                WritableUtils.writeVLong(out, l);
                l = Long.parseLong(column_line_parts[4]);
                WritableUtils.writeVLong(out, l);

                DataInputStream in;
                File valbufFile = new File(args[0] + "/h0_valbuf" + column_line_parts[1] + ".fix");
                long fileSize = 0;
                if (valbufFile.exists()) {
                    in = new DataInputStream(new FileInputStream(args[0] + "/h0_valbuf" + column_line_parts[1] + ".fix"));
                    fileSize = valbufFile.length();
                } else {
                    valbufFile = new File(args[0] + "/h0_valbuf" + column_line_parts[1]);
                    if (valbufFile.exists()) {
                        in = new DataInputStream(new FileInputStream(args[0] + "/h0_valbuf" + column_line_parts[1]));
                        fileSize = valbufFile.length();
                    } else {
                        System.out.println(args[0] + "/h0_valbuf" + column_line_parts[1] + " not found, please run rcf.pl first!");
                        return;
                    }
                }
                byte data[] = new byte[Integer.parseInt(column_line_parts[4])];
                int read_cnt = (int)fileSize;
                if (Integer.parseInt(column_line_parts[4]) < fileSize) {
                    read_cnt = Integer.parseInt(column_line_parts[4]);
                }
                in.readFully(data, 0, read_cnt);
                in.close();
                out.write(data);
            }
        } catch (IOException | NumberFormatException e) {
        } finally {
            report.close();
            out.close();
        }
    }
}
