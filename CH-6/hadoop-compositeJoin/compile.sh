cd src
javac -cp ../lib/commons-cli-1.2.jar:../lib/hadoop-core-1.0.4.jar CompositeJoin.java
jar cvf CompositeJoin.jar *.class
cp CompositeJoin.jar ../
