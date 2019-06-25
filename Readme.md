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

## Simple Test ( 5 repetitions ) with 1M requests, 1 Threads, datasize of 2048, buffersize of 1048576
```console
mvn clean package && java -jar target/benchmarks.jar "KryoBenchmark"  -i 5 -wi 0 -f 1 -t 1  -p buffersize=1048576 -p datasize=2048
```

```console
(...)

```
