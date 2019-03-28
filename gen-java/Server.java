import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Server {
    public static SentimentHandler handler;
    private static SentimentAnalyzerService.Processor<SentimentHandler> processor;
    public static Properties prop;

    public static void main(String[] args) {
        try {
            prop = new Properties();
            InputStream is = new FileInputStream("sentiment.cfg");
            prop.load(is);
            handler = new SentimentHandler(prop);
            processor = new SentimentAnalyzerService.Processor<>(handler);

            startThreadPoolServer();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void startThreadPoolServer() {
        try {
            // Create Thrift server socket as a thread pool
            Integer serverPort = Integer.valueOf(prop.getProperty("server.port"));
            TServerTransport serverTransport = new TServerSocket(serverPort);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);
            TServer server = new TThreadPoolServer(args);

            System.out.println("Starting the SentimentAnalyzer Server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

