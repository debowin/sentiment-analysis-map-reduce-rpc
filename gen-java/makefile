JFLAGS = -cp
JVM = java
JC = javac
DEST = .
.SUFFIXES: .java .class
CLASSPATH = .\:/usr/local/Thrift/*

CLASSES = \
          Server.java \
          Client.java \
          ComputeNode.java

.java.class:
	$(JC) $(JFLAGS) $(CLASSPATH) $*.java -d $(DEST)

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

srv: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) Server

cli: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) Client

nod: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) ComputeNode $(ID)

.PHONY: srv cli nod