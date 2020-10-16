hadoop com.sun.tools.javac.Main Outlier.java
jar cf wc.jar Outlier*.class
hadoop jar wc.jar Outlier $1 $2 $3 $4