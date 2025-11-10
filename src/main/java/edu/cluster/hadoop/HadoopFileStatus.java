package edu.cluster.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
  public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println("Usage: HadoopFileStatus <path> <filename> <new_filename>");
            System.exit(1);
        }

        String dirPath = args[0];
        String fileName = args[1];
        String newFileName = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(dirPath, fileName);

        if (!fs.exists(filePath)) {
            System.out.println("File does not exist: " + filePath);
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filePath);

        System.out.println("File Name: " + status.getPath().getName());
        System.out.println("File Size: " + status.getLen() + " bytes");
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Permission: " + status.getPermission());
        System.out.println("Replication: " + status.getReplication());
        System.out.println("Block Size: " + status.getBlockSize());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println("Block offset: " + blockLocation.getOffset());
            System.out.println("Block length: " + blockLocation.getLength());
            System.out.print("Block hosts: ");
            for (String host : blockLocation.getHosts()) {
                System.out.print(host + " ");
            }
            System.out.println();
        }

        // Rename the file
        Path newPath = new Path(dirPath, newFileName);
        boolean renamed = fs.rename(filePath, newPath);
        if (renamed) {
            System.out.println("File renamed to: " + newFileName);
        } else {
            System.out.println("Rename failed.");
        }

        fs.close();
    }
}