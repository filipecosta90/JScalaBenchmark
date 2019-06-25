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
# Run complete. Total time: 00:13:02

REMEMBER: The numbers below are just data. To gain reusable insights, you need to follow up on
why the numbers are the way they are. Use profilers (see -prof, -lprof), design factorial
experiments, perform baseline and negative tests that provide experimental control, make sure
the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
Do not assume the numbers tell you what you want them to tell.

Benchmark                 (blockSize)  (buffersize)  (colsize)  (ncols)   Mode  Cnt   Score   Error   Units
KryoBenchmark.testMethod         1000             1         36      400  thrpt    5  30.228 ± 2.328  ops/ms
KryoBenchmark.testMethod         1000             1         36      400   avgt    5   0.033 ± 0.002   ms/op
```
