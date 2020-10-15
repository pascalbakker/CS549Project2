hadoop com.sun.tools.javac.Main KMeans.java
jar cf wc.jar KMeans*.class
hadoop jar wc.jar KMeans $1 $2 $3