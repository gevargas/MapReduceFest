rm -rf *.class
javac -g -cp ../../lib/hadoop-core-1.0.4.jar  *.java
jar cvf DistributedGrep.jar *.class

