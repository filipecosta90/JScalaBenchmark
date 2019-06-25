# JScalaBenchmark

Microbenchmarking with JMH


Serializers included:
- [kryo](https://github.com/EsotericSoftware/kryo)

## Getting started 
```
git clone https://github.com/filipecosta90/JScalaBenchmark.git
cd JScalaBenchmark
mvn clean package
```

## 5 repetitions of 60 seconds Test with 100K Rows, 400 columns, column size of 36 Bytes, 1 Threads, with an heap size of 8G
```console
java -jar target/benchmarks.jar "KryoBenchmark"  -i 5 -wi 0 -f 1 -t 1 -jvmArgs="-Xms8G -Xmx8G"
```

```console
(...)
```
