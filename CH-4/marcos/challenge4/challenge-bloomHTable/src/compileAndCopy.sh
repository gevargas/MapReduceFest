rm -rf *.class
javac -cp ../../lib/hadoop-core-1.0.4.jar:../../lib/commons-lang-2.4.jar:../../lib/hbase-0.94.7.jar  *.java
jar cvf BloomFilterHTable.jar *.class
