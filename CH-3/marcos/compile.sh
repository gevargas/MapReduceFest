cd src
javac -cp ../../hadoop-core-1.0.4.jar:../lib/commons-lang-2.4.jar *.java
jar cvf WikipediaIndex.jar *.class
cp WikipediaIndex.jar ../
