rm -rf *.class
javac -g -cp ../lib/*:../lib:. *.java
jar cvf BloomFilterHTable.jar *.class
jar cvf BloomFilterHTableDriver.jar BloomFilterDriverHTable.class MRDPUtils.class