-----
NOTES
-----

### encountered difficulties

- high disk IO speed demand of the program
 - need to use ramdisk on local flink cache, JVM and tuning JVM parameters to right version to increase IO speed and processing speed
 - improved processing 1GB data time from ~ days to just ~ 10 mins by using the framework and ramdisk
 - possibly need to add disturbuted RAM in the future

- unreal and inconsistent (dirty) data:
 - contain unexpected behaviors (inconsistent num. of columns, zero trades, repeated timestamp, invalid data formats, etc)
  - solution: need to use pandas to clean data

- careful computation on longest times on different metric each day but not overall days 

- large dev. cost on beam flink framework
 - long time on file reading on Beam Java: ~ 1 hr
 - correct data structure computation and lots of unsolved runner errors using Beam framework
 - high memory loading of wrong data model design decisions
 - time to have correct flink config to ensure it is runnable

- fix Beam Framework API issues & common bugs, mainly on coder encoding algorithms and jvm Memory issues

- research on compilable, serializable and optimized auto-encodable BEAM data scheme and coder for large loading computations

- Beam Framework API unstable issues like unstable coder problem, need special algorithm on encoding

- Beam App java.lang.OutOfMemoryError: Java heap space during processing large data files
  - solution: using Flink runner to split off and store the large bundle of input data during processing

- long time data processing
  - solution: using modern fast streaming parallel processing library apache beam instead of simple threading

- streaming processing design study using modern streaming API
  - solution: need to research and do testings on Beam API

- freq. codebase refactoring due to API research updates

- time cost on well test designs & debugs on streaming processing and method of failures handling
  - solution: splitting named processing steps with well junit tests

- optimization on unit tests and code base refactoring

- multi programs version tuning: solving java & maven plugins versions issues on running Apache Beam apps
 - flink versioning: flink 1.14, 1.16 is not working or work well
