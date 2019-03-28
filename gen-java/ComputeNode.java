import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

// Generated code
public class ComputeNode {
    public static ComputeHandler handler;
    private static ComputeService.Processor<ComputeHandler> processor;
    public static Properties prop;

    public static void main(String[] args) {
        try {
            prop = new Properties();
            InputStream is = new FileInputStream("sentiment.cfg");
            prop.load(is);

            // read node id from cli
            Integer nodeIndex = Integer.valueOf(args[0]);
            handler = new ComputeHandler(prop, nodeIndex);
            processor = new ComputeService.Processor<>(handler);

            startThreadPoolServer(nodeIndex);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void startThreadPoolServer(Integer nodeIndex) {
        try {
            Integer serverPort = Integer.valueOf(prop.getProperty("computenode.port"));
            // Create Thrift server socket as a thread pool
            TServerTransport serverTransport = new TServerSocket(serverPort);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);
            TServer server = new TThreadPoolServer(args);

            System.out.printf("Starting the ComputeNode(ID: %d)...\n", nodeIndex);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

