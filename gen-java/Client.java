import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class Client {
    public static void main(String[] args) {
        try {
            // Get Configs
            Properties prop = new Properties();
            InputStream is = new FileInputStream("sentiment.cfg");
            prop.load(is);
            String serverAddress = prop.getProperty("server.address");
            Integer serverPort = Integer.valueOf(prop.getProperty("server.port"));
            String inputPath = prop.getProperty("input.path");
            File inputDir = new File(inputPath);
            List<File> inputFiles = Arrays.asList(Objects.requireNonNull(inputDir.listFiles()));

            // create client connection
            TTransport transport = new TSocket(serverAddress, serverPort);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            SentimentAnalyzerService.Client client = new SentimentAnalyzerService.Client(protocol);

            // Ping
            client.ping();
            // Call the Sentiment Analysis Service
            List<String> ipFileNames = inputFiles.stream().filter(File::isFile)
                    .map(File::getAbsolutePath).collect(Collectors.toList());
            String opFileName = client.getSentiments(ipFileNames);
            // Display Output to Console
            System.out.println("!*=====================*! Sentiment Analysis Output !*=====================*!");
            try (BufferedReader br = new BufferedReader(new FileReader(opFileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            }

            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
