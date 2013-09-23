rm -rf *.class
javac -g -cp ../../lib/hadoop-core-1.0.4.jar:../../lib/commons-lang-2.4.jar  *.java
jar cvf BloomFilter.jar *.class
jar cvf BloomFilterDriver.jar BloomFilterDriver.class
