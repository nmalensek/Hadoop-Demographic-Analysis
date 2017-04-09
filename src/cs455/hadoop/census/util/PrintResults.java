package cs455.hadoop.census.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class PrintResults {

    private Path outputPath;
    private FileSystem fileSystem;
    private Configuration configuration;

    public PrintResults(Path outputPath, Configuration configuration) throws IOException {
        this.outputPath = outputPath;
        this.configuration = configuration;
        fileSystem = FileSystem.get(configuration);
    }

    public void printOutput() throws IOException {
        BufferedReader reader = null;
        try {
            FileStatus[] files = fileSystem.listStatus(outputPath);
            for (FileStatus status : files) {
                Path path = status.getPath();
                if (path.getName().contains("question") && !path.getName().contains("question9")) {
                    reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                    String line = reader.readLine();

                    while (line != null) {
                        System.out.println(line);
                        line = reader.readLine();
                    }
                }
            }
        } finally {
            reader.close();
        }
    }
}
