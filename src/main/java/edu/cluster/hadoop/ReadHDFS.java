package edu.cluster.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <file_path>");
            System.exit(1);
        }

        String filePath = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("File does not exist: " + filePath);
            System.exit(1);
        }

        FSDataInputStream inStream = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        String line;
        System.out.println("----- File Content -----");
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        inStream.close();
        fs.close();
    }
}