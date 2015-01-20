rm KMeansCom/* -r
javac -classpath /opt/haloop/build/hadoop-0.20.2-dev-core.jar -d KMeansCom KMeansCom.java
jar -cvf KMeansCom.jar -C KMeansCom .
