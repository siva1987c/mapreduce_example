rm PageRank/* -r
javac -classpath /opt/haloop/build/hadoop-0.20.2-dev-core.jar -d PageRank PageRank.java
jar -cvf PageRank.jar -C PageRank .
