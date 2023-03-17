-----------------
QUANT REPORT APP
-----------------

### project features

- research on apache beam flink framework on scaling, parallel streamming, immutable, fast and batch processing of large amount of data (~ GB - TB) in a fast and parallel way
- simulate quants' daily data analysis job

### main results
- successfully analyse the 100 Scandinavian blue chip stocks 4 days data just in several minutes using big data toolings, i.e. Apache Beam Flink Runner, etc

### main frameworks

- apache beam with flink runner

### hardware dependencies
- ~ 20GB RAM + i7 / i9 PC

### software dependencies
- ubuntu 20.04.5 LTS
- docker 2010.12
  - mvnd
  - Java 11
  - postgresql 12.13
  - Python 3.7
  - Maven 3.8.6 running on Java 11
  - Git 2.25.1

### how to run the app?

```sh

# test
./test.sh

# put the input file in ./inputs folder and change the input file name in the source code to 'scandi.csv' (~ TB) or 'test.csv' (~ MB) and execute commands as below
./setup.sh

# finally, the `QuantStockReports.json` file will be generated on the ./outputs/ path after ~ mins
# >> ./output/QuanStockReports.json

# reset and clear all outputs
./reset

```

### syntax remarks

- []: undone
- [ ]: processing
- [o]: done
- [-]: onhold
- [x]: cancelled

### requirements

- large data loader research:
  - ~ TB large file data loading test: [splink runner] vs [direct runner] vs [java native reader] performance in loading \*.csv
    - [o] native java reader: FAILED !!
    - [o] apache beam direct loader + data reading + no log: FAILED !!
    - [o] apache beam direct loader + no data reading: 280 seconds (~ 5 mins), SUCCESS !!
    - [o] apache beam flink loader + data reading + no log: 731s (~ 10 mins), SUCCESS !!
    - [o] apache beam flink loader + data reading + log: (35min), SUCCESS !!
    - [o] apache beam flink loader + data reading + data processing + no log: FAILED !!
    - [ ] custom parallel runner + direct loader + data reading + processing + log:

- common features with tests
  - features
    - [o] pardo serializable function test
    - [o] filter valid non auction trading data
    - [o] tick changes computation
    - [o] json reporting using Jackson ObjectMapper with lombok / Beam schemer
    - [o] last digits
    - [o] round & last digits Prob.
    - [o] format double str. to int str.
    - [o] parallel sort
    - [o] parallel merge collections
    - [o] group and sort
    - [o] mean
    - [o] median
    - [o] max
    - [o] diff
    - [o] maven surefire test reporting
    - [o] jdk logging config string formatting with threading id context
    - [o] make all processing function serializable to cloud computing 
    - [-] immutable view indexing for parallel computation (replaced by local threading)
    - [o] objectMapper json reporting
    - [x] data scheme design: using BEAM data scheme
    - [o] surefire parallel testing setup

  - [x] spring with docker deployment with beam flink runner
    - [-] embedded flink runner
    - [-] main libs, logics & junit testing migrations
    - [o] mvn dev & gradle deployment with docker

  - [o] JUnit test with apache beam test pipeline

  - [o] local ramdisk toolings to increase data loading and disk IO speed greatly

- algorithms part
  - [o] stock grouping
  - [o] stock sorting (using local parallel threading)
  - [o] stock data filter
  - [o] Mean time between trades
  - [o] Median time between trades
  - [o] Mean time between tick changes
  - [o] Median time between tick changes
  - [o] Longest time between trades
  - [o] Longest time between tick changes
  - [o] Mean bid ask spread
  - [o] Median bid ask spread
  - [o] Examples of the round number effect - (both in traded values and traded volumes - i.e. is there an increased probability of the last digit on prices being a 0 compared to other last digits)

- development toolings & libs versions
  - [o] git versioning
  - [-] mvn build system with jython + freezed java infra. layer to enable no recompiled hot reloading dev / build system switching to gradle with build cache, app features parallel dev. & testing using containers system like docker to speed up
