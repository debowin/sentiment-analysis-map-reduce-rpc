# MAP-REDUCE FRAMEWORK WITH THRIFT RPC FOR SENTIMENT ANALYSIS



This system can perform a very simple form of Sentiment Analysis on very large datasets by utilising the Map-Reduce paradigm.
The client node can send a job to the server node with a list of files, which the server breaks down into map tasks and assigns to
each compute node, who are able to perform multithreaded sentiment analysis on each file and return the scores to the server node. 
Once all the scores are received at the server node, it assigns a sort task to one compute node which sorts all the results by 
sentiment score in decreasing order and returns this to the client node via the server node.

Each Compute Node is allotted a Load Probability to reject tasks if using Load Balancing scheduling and also for injecting loads
through configurable delays. For more information, look at "problem_statement.pdf".

The code is very well documented with very verbose logging.
Future Work would be on better Exception Handling.

## CONFIGURATION

This Java Properties File(sentiment.cfg) stores many of the values needed to dynamically configure the
system, including:
* addresses and port numbers of each of the node types - client, server and compute.
* which compute node to use as sort node(index).
* load probabilities of each compute node - for load balancing and injection.
* paths to the input, intermediate and output directories.
* scheduling policy - LOAD BALANCING or RANDOM.
* paths of the positive and negative vocabulary files.

## SAMPLE DATA

Some sample data has been provided in the ./data folder. It also contains sample positive and negative vocabulary files.

## INSTRUCTIONS

There is a makefile provided that can be used to easily build and run the project.
*YOU NEED JAVA 1.8+ AND THRIFT 0.9+ TO RUN THIS SYSTEM*

* Modify the classpath variable in the makefile according to your system.
* Check to make sure the address, ports and other options are as desired in the
sentiment.cfg file.
* Run “make clean && make” to rebuild the source.
* Run “make cli” to run the client.
* Run “make srv” to run the server.
* Run “make nod ID=<id>” to run the compute nodes
	* <id> starts at 0 and should be set based on what load probability you want for
that node.
	* <id> is used to index into the comma separated list of load probabilities provided
	in the config.