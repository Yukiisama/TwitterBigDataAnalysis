# Twitter Analatyca

<p  align="center">

<a  href="https://github.com/Yukiisama/TwitterBigDataAnalysis"><img  src="https://badgen.net/badge/icon/Twitter Related?icon=twitter&label"  alt="Twitter"></a>

<a  href="https://github.com/Yukiisama/TwitterBigDataAnalysis"><img  src="https://badgen.net/badge/icon/Built using Maven?icon=maven&label"  alt="Built using Maven"></a>



</p>



## Setup environment
You currently need to be on the "Large-Echelle" cluster in dedicated room to execute this project.


```bash
$ source /espace/Auber_PLE-203/user-env.sh
```

## Run the Batch Layer
  

 ```
$ cd spark_maven
$ mvn package
```

### Using YARN

```$ spark-submit --master yarn target/TPSpark-0.0.1.jar```

### Otherwise

```$ spark-submit target/TPSpark-0.0.1.jar --deploy-mode cluster```

## Optimizations possibilities

There is quite a few optimizations that could be made using custom JVM arguments.

You can use them by using the following runtime argument:
```bash
 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps ... othersArgs" 
```

In the future, it could be directly configurated inside the mainclass by setting variable with the Java Spark Configuration.