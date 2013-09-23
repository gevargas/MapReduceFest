cd src
javac -cp ../../hadoop-core-1.0.4.jar *.java
jar cvf PublicDataAnalysis.jar *.class
cp WordCount.jar ../