import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ComputeHandler implements ComputeService.Iface {
    private Properties prop;
    private Set<String> positives;
    private Set<String> negatives;
    // for load probability related stuff
    private List<Long> timers;
    private Scheduler scheduler;
    private Float loadProb;
    private Random rand = new Random();
    private Integer loadDelay;

    /**
     * Constructor - Sets Properties, Scheduler, LoadProb according to nodeIndex, populates
     * the vocab sets.
     *
     * @param properties java properties for config file
     * @param nodeIndex used to get corresponding load probability from config file
     */
    ComputeHandler(Properties properties, Integer nodeIndex) {
        prop = properties;
        timers = new CopyOnWriteArrayList<>();
        scheduler = Scheduler.valueOf(prop.getProperty("scheduler.policy"));
        loadProb = Float.valueOf(Arrays.asList(prop.getProperty("mapnode.loadprob")
                .split("\\s*,\\s*")).get(nodeIndex));
        loadDelay = Integer.valueOf(prop.getProperty("load.delay"));
        populateVocab();
    }

    /**
     * Fill up the positive and negative word sets.
     */
    private void populateVocab() {
        try {
            positives = new HashSet<>();
            Path vocabFile = Paths.get(prop.getProperty("vocab.positive"));
            positives.addAll(Files.readAllLines(vocabFile));
            negatives = new HashSet<>();
            vocabFile = Paths.get(prop.getProperty("vocab.negative"));
            negatives.addAll(Files.readAllLines(vocabFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Basic Ping service
     *
     * @return true
     * @throws TException
     */
    @Override
    public boolean ping() throws TException {
        System.out.println("Ping Received");
        return true;
    }

    /**
     * mapTask service - accept or reject based on loadProb,
     * launch map task in a new thread if accepted.
     *
     * @param fileName input filename to process
     * @return true if accept, false if reject
     * @throws TException
     */
    @Override
    public boolean mapTask(String fileName) throws TException {
        if (scheduler == Scheduler.LOAD_BALANCING && rand.nextFloat() < loadProb) {
            // reject task with probability = loadProb
            System.out.printf("[TID: %d] Call to mapTask(%s) rejected.\n", Thread.currentThread().getId(), fileName);
            return false;
        }
        System.out.printf("[TID: %d] Call to mapTask(%s) accepted.\n", Thread.currentThread().getId(), fileName);
        Runnable runMapTask = () -> runMapTask(fileName);
        new Thread(runMapTask).start();
        return true;
    }

    /**
     * sortTask service - sort the input filenames by score from the list of
     * intermediate files and write this to an output file.
     *
     * @param fileNames intermediate filenames to sort
     * @return output filename
     * @throws TException
     */
    @Override
    public String sortTask(List<String> fileNames) throws TException {
        try {
            System.out.printf("[TID: %d] Call to sortTask().\n", Thread.currentThread().getId());
            Instant start = Instant.now();
            Map<String, Float> fileScoreMap = new HashMap<>();
            List<Path> intermediateFiles = fileNames.stream().map(Paths::get).collect(Collectors.toList());
            for (Path file : intermediateFiles) {
                // process each intermediate file
                String[] record = new String(Files.readAllBytes(file)).split(",\\s");
                fileScoreMap.put(record[0], Float.valueOf(record[1]));
            }
            List<Map.Entry<String, Float>> fileScoreList = new ArrayList<>(fileScoreMap.entrySet());
            // sort the list of records by score in descending order
            fileScoreList.sort(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));
            // write this to output file
            Path outputFile = Paths.get(prop.getProperty("output.path"), System.currentTimeMillis() + "_output.txt");
            for (Map.Entry<String, Float> fileScore : fileScoreList) {
                Files.write(outputFile, String.format("%s, %f\n", fileScore.getKey(), fileScore.getValue()).getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            }
            Instant end = Instant.now();
            System.out.printf("Sort Task Complete! Time Taken: %d ms.\n", Duration.between(start, end).toMillis());
            return outputFile.toAbsolutePath().toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * execute the map task by calculating sentiment score and writing it
     * to an intermediate file and sending the name back to the server.
     * Also inject load with probability = loadProb.
     *
     * @param fileName input filename to process
     */
    private void runMapTask(String fileName) {
        try {
            Instant start = Instant.now();
            if (rand.nextFloat() < loadProb) {
                // inject load with probability = loadProb
                Thread.sleep(loadDelay);
            }
            Float sentimentScore = calculateSentimentScore(fileName);
            String rawFileName = Paths.get(fileName).getFileName().toString();
            // write results to intermediate file
            Path intermediateFile = Files.createFile(Paths.get(prop.getProperty("intermediate.path"),
                    System.currentTimeMillis() + "_" + rawFileName));
            Files.write(intermediateFile, String.format("%s, %f", fileName, sentimentScore).getBytes());
            Instant end = Instant.now();
            timers.add(Duration.between(start, end).toMillis());
            System.out.printf("Map Task(%d) Complete: (%s, %f). Avg Time Taken: %.2f ms.\n", timers.size(),
                    fileName, sentimentScore, timers.stream().mapToLong(v -> v).average().orElse(0.0));
            String serverAddress = prop.getProperty("server.address");
            Integer serverPort = Integer.valueOf(prop.getProperty("server.port"));
            // make the return RPC call to return results to server
            TTransport transport = new TSocket(serverAddress, serverPort);
            transport.open();
            SentimentAnalyzerService.Client client = new SentimentAnalyzerService.Client(new TBinaryProtocol(transport));
            client.returnSentimentResult(intermediateFile.toAbsolutePath().toString());

            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * calculating sentiment score for that file using the formula = (p-n)/(p+n)
     * by counting pos and neg words in the input file
     *
     * @param fileName input filename to score
     * @return sentiment score
     */
    private Float calculateSentimentScore(String fileName) {
        try {
            Path inputFile = Paths.get(fileName);
            String content = new String(Files.readAllBytes(inputFile))
                    .toLowerCase().replace("--", " ");
            int numPos = 0;
            int numNeg = 0;
            Matcher matcher = Pattern.compile("([a-zA-Z\\-]+)")
                    .matcher(content);
            while (matcher.find()) {
                String word = matcher.group();
                if (positives.contains(word)) {
                    numPos += 1;
                }
                if (negatives.contains(word)) {
                    numNeg += 1;
                }
            }
            return (numPos - numNeg) / (float) (numPos + numNeg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0.0f;
    }

    /**
     * Scheduler Modes
     */
    enum Scheduler {
        RANDOM, LOAD_BALANCING
    }
}