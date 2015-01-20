rm KMeansOrg/* -r
javac -classpath /opt/haloop/build/hadoop-0.20.2-dev-core.jar -d KMeansOrg KMeansOrg.java
jar -cvf KMeansOrg.jar -C KMeansOrg .
