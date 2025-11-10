package edu.cluster.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println("Usage: WriteHDFS <file_path> <text>");
            System.exit(1);
        }

        String filePath = args[0];
        String text = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            FSDataOutputStream outStream = fs.create(path);
            outStream.writeUTF("Bonjour tout le monde !\n");
            outStream.writeUTF(text + "\n");
            outStream.close();
            System.out.println("File created successfully: " + filePath);
        } else {
            System.out.println("File already exists: " + filePath);
        }

        fs.close();
    }
}
