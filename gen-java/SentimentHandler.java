import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class SentimentHandler implements SentimentAnalyzerService.Iface {
    private List<String> computeNodes;
    private List<String> intermediateFiles;
    private Integer mapTasksCount;
    private Integer mapTasksComplete;
    private Properties prop;
    private Integer computeNodePort;

    /**
     * Constructor - Get the list of compute nodes available and port
     * @param properties java properties for config file
     */
    SentimentHandler(Properties properties) {
        prop = properties;
        computeNodePort = Integer.valueOf(prop.getProperty("computenode.port"));
        computeNodes = Arrays.asList(prop.getProperty("mapnode.address").split("\\s*,\\s*"));
        System.out.printf("Compute Nodes: %s\n", computeNodes);
    }

    /**
     * basic ping service
     * @return true
     * @throws TException
     */
    @Override
    public boolean ping() throws TException {
        System.out.println("Ping Received");
        return true;
    }

    /**
     * getSentiments service - handles the entire job - map, sort and return.
     * @param fileNames input filenames to score
     * @return output filename containing sorted list by scores
     * @throws TException
     */
    @Override
    public String getSentiments(List<String> fileNames) throws TException {
        try {
            intermediateFiles = new ArrayList<>();
            mapTasksCount = fileNames.size();
            mapTasksComplete = 0;
            System.out.printf("[TID: %d] Call to getSentiments().\nSplitting into %d Map Tasks.\n",
                    Thread.currentThread().getId(), mapTasksCount);
            Instant start = Instant.now();
            runMapTasks(fileNames);
            // wait until all map tasks have completed
            synchronized (this) {
                try {
                    System.out.println("Waiting for all Map Tasks to complete...");
                    while (mapTasksComplete < mapTasksCount) {
                        wait();
                    }
                    System.out.println("All Map Tasks Completed! Proceeding to the Sort Task.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // perform sorting
            String outputFileName = runSortTask();
            Instant end = Instant.now();
            String timeTaken = String.format("Total Time Taken: %d ms.\n", Duration.between(start, end).toMillis());
            System.out.println("Completed Sentiment Analysis Job! " + timeTaken);

            // sppend total runtime of job to output file
            Files.write(Paths.get(Objects.requireNonNull(outputFileName)), timeTaken.getBytes(), StandardOpenOption.APPEND);
            return outputFileName;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * calls sort service on a designated compute node
     * @return output filename
     */
    private String runSortTask() {
        try {
            String computeNodeAddress = computeNodes.get(Integer.valueOf(prop.getProperty("sortnode.index")));
            //Create client connect.
            TTransport transport = new TSocket(computeNodeAddress, computeNodePort);
            transport.open();
            ComputeService.Client client = new ComputeService.Client(new TBinaryProtocol(transport));
            System.out.printf("Launching Sort Task on %s.\n", computeNodeAddress);
            String outputFileName = client.sortTask(intermediateFiles);
            transport.close();
            return outputFileName;
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * returnSentimentResult service - serves as a return RPC call for compute node to send back
     * sentiment score results to the server
     * @param fileName intermediate filename
     * @throws TException
     */
    @Override
    public synchronized void returnSentimentResult(String fileName) throws TException {
        mapTasksComplete += 1;
        intermediateFiles.add(fileName);
        System.out.printf("[TID: %d] (%d/%d) Map Task Complete: %s\n", Thread.currentThread().getId(),
                mapTasksComplete, mapTasksCount, fileName);
        if (mapTasksComplete.equals(mapTasksCount)) {
            // if all map tasks complete, notify waiting thread
            notify();
        }
    }

    /**
     * split the job into several tasks, assigning compute nodes at random
     * if task is rejected try again until accepted. invoke maptask on chosen nodes.
     * @param fileNames input filenames
     */
    private void runMapTasks(List<String> fileNames) {
        // one task per file
        for (String fileName : fileNames) {
            boolean accepted = false;
            while (!accepted) {
                try {
                    // choose a compute node at random
                    String computeNodeAddress = getRandomComputeNode();
                    //Create client connect.
                    TTransport transport = new TSocket(computeNodeAddress, computeNodePort);
                    transport.open();
                    ComputeService.Client client = new ComputeService.Client(new TBinaryProtocol(transport));
                    accepted = client.mapTask(fileName);
                    System.out.printf("Launching Map Task(%s) on %s %s!\n", fileName,
                            computeNodeAddress, accepted ? "succeeded" : "failed");
                    transport.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    accepted = false;
                }
            }
        }
    }

    /**
     * does exactly as the name says - pick a random compute node for map tasks.
     * @return hostname of chosen node
     */
    private String getRandomComputeNode() {
        Random rand = new Random();
        int mapNodeIndex = rand.nextInt(computeNodes.size());
        return computeNodes.get(mapNodeIndex);
    }
}
