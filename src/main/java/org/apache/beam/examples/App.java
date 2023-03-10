package org.apache.beam.examples;

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.NonNull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// import lombok.extern.log4j.Log4j2;

import org.joda.time.Duration;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.utility.DockerImageName;

// @Log4j2
// @Slf4j
// @Bean
@Configuration
@PropertySource(name = "test-java-app", value = "classpath:config.properties")
public class App {

  // private static final String _inputFiles = "./inputs/test.csv";

  private static final boolean DEBUG = false;

  private static org.slf4j.Logger log = LoggerFactory.getLogger(App.class);

  /** logging */

  /** set of ParDo API transforms */
  public static class MapElementsTransforms {

    public static class FunctionOfAssertNotNull
      implements SerializableFunction<Iterable<Long>, java.lang.Void> {

      @Override
      public Void apply(Iterable<Long> ints) {
        assertTrue(Iterables.size(ints) > 0);
        return null;
      }
    }

    public static class SimpleFunctionOfTrades
      extends SimpleFunction<KV<String, List<KV<Integer, Event>>>, KV<String, List<Double>>> {

      private Event.Key tradeEventProperty;

      public SimpleFunctionOfTrades(Event.Key tradeEventProperty) {
        this.tradeEventProperty = tradeEventProperty;
      }

      @Override
      public KV<String, List<Double>> apply(
        KV<String, List<KV<Integer, Event>>> stockEvent
      ) {
        List<Double> o = stockEvent
          .getValue()
          .stream()
          .parallel()
          // filter only update type == trade
          .filter(kv -> kv.getValue().getUpdateType() == 1)
          // get corr. value
          .map(kv -> {
            Event e = kv.getValue();
            if (tradeEventProperty == Event.Key.TradePrice) return new Double(
              (double) e.getTradePrice()
            );
            if (tradeEventProperty == Event.Key.TradeVolume) return new Double(
              (double) e.getTradeVolume()
            );
            if (tradeEventProperty == Event.Key.Time) return new Double(
              (double) e.getDateTime()
            );
            return null;
          })
          .collect(Collectors.toList());

        return KV.of(stockEvent.getKey(), o);
      }
    }

    public static class SimpleFunctionOfBidAskSpreads
      extends SimpleFunction<KV<String, List<KV<Integer, Event>>>, KV<String, List<Double>>> {

      @Override
      public KV<String, List<Double>> apply(
        KV<String, List<KV<Integer, Event>>> stockEvent
      ) {
        // if (DEBUG){

        // String TAG = App.Helper.randomCallerTag();

        // log.info(TAG + " # args " + stockEvent.toString());

        // }

        List<Double> o = stockEvent
          .getValue()
          .stream()
          .parallel()
          .map(kv -> kv.getValue())
          .map((Event e) -> {
            return (double) Math.abs((e.getBidPrice()) - (e.getAskPrice()));
          })
          .collect(Collectors.<Double>toList());
        return KV.of(stockEvent.getKey(), o);
      }
    }

    public static class SimpleFunctionOfFilterNonValidTradingData
      extends SimpleFunction<KV<String, List<KV<Integer, Event>>>, KV<String, List<KV<Integer, Event>>>> {

      @Override
      public KV<String, List<KV<Integer, Event>>> apply(
        KV<String, List<KV<Integer, Event>>> input
      ) {
        String TAG = App.Helper.randomCallerTag();

        if (DEBUG) {
          log.info(TAG + "# args");

          log.info(input.toString());
        }

        KV<String, List<KV<Integer, Event>>> RESULT = null;

        List<KV<Integer, Event>> result = input
          .getValue()
          .stream()
          .parallel()
          .filter(kv -> {
            Event event = kv.getValue();

            // condition codes filter
            String conditionCode = event.getCondition();

            // spread filter
            double askPrice = event.getAskPrice();

            double bidPrice = event.getBidPrice();

            double tradePrice = event.getTradePrice();

            // ensure normal trading times
            // ensure normal bidding prices
            if (
              !(
                conditionCode.equals(Event.NULL_STRING_REPR) ||
                conditionCode.equals("XT")
              ) ||
              !(bidPrice <= tradePrice && tradePrice <= askPrice)
            ) return false;

            return true;
          })
          .collect(Collectors.toList());

        RESULT = KV.of(input.getKey(), result);

        if (DEBUG) {
          log.info(TAG + "# results");

          log.info(RESULT.toString());
        }

        return RESULT;
      }
    }

    public static class SimpleFunctionOfKvSort<T>
      extends SimpleFunction<KV<String, Iterable<KV<Integer, T>>>, KV<String, List<KV<Integer, T>>>> {

      @Override
      public KV<String, List<KV<Integer, T>>> apply(
        KV<String, Iterable<KV<Integer, T>>> input
      ) {
        String TAG = App.Helper.randomCallerTag();

        if (DEBUG) log.info(TAG + ": " + input.toString());

        List<KV<Integer, T>> result = App.<T>sortKVList(input.getValue());

        if (DEBUG) log.info(TAG + ": " + result.toString());

        return KV.of(input.getKey(), result);
      }
    }

    public static class SimpleFunctionOfBuildingReport
      extends SimpleFunction<KV<String, Iterable<ReportResult>>, ReportResult> {

      @Override
      public ReportResult apply(KV<String, Iterable<ReportResult>> o) {
        return Helper.DataTypes
          .iterableToList(o.getValue())
          .stream()
          .parallel()
          .reduce(
            ReportResult.builder().stockCode(o.getKey()).build(),
            (ReportResult accResult, ReportResult currentResult) -> {
              return accResult.addResult(currentResult);
            }
          );
      }
    }

    public static class SimpleFunctionOfComputeReportResult
      extends SimpleFunction<KV<String, Double>, KV<String, ReportResult>> {

      private SerializableBiFunction<ReportResult, Double, ReportResult> attachEventResult;

      public SimpleFunctionOfComputeReportResult(
        SerializableBiFunction<ReportResult, Double, ReportResult> attachEventResult
      ) {
        super();
        this.attachEventResult = attachEventResult;
      }

      @Override
      public KV<String, ReportResult> apply(KV<String, Double> o) {
        String TAG = App.Helper.randomCallerTag();

        if (DEBUG) log.info(TAG + ": " + o.toString());

        return KV.of(
          o.getKey(),
          this.attachEventResult.apply(
              ReportResult.builder().build(),
              o.getValue()
            )
        );
      }
    }

    public static class SimpleFunctionOfComputeReportResultWithSideComputation
      extends SimpleFunction<KV<String, List<Double>>, KV<String, ReportResult>> {

      private BiFunction<ReportResult, Double, ReportResult> attachEventResult;

      private SerializableFunction<List<Double>, Double> sideComputation;

      public SimpleFunctionOfComputeReportResultWithSideComputation(
        @NonNull BiFunction<ReportResult, Double, ReportResult> attachEventResult,
        @NonNull SerializableFunction<List<Double>, Double> sideComputation
      ) {
        super();
        this.attachEventResult = attachEventResult;
        this.sideComputation = sideComputation;
      }

      @Override
      public KV<String, ReportResult> apply(KV<String, List<Double>> o) {
        String TAG = App.Helper.randomCallerTag();

        if (DEBUG) log.info(TAG, o.toString());

        // assertTrue(o.getValue().size() > 0);

        return KV.of(
          o.getKey(),
          this.attachEventResult.apply(
              ReportResult.builder().build(),
              (o.getValue().size() == 0)
                ? null
                : this.sideComputation.apply(o.getValue())
            )
        );
      }
    }
  }

  public static class ParDoTransforms {

    public static class DoFnOfParseEvent
      extends DoFn<String, KV<String, KV<Integer, Event>>> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        String csvLine = c.element();

        Event event = Event.parseFromSourceLineString(csvLine, null);

        // log.info(event.toString());

        c.output(
          KV.of(
            // stock code
            event.getStockCode(),
            KV.of(
              // datetime
              event.getDateTime(),
              // stock event
              event
            )
          )
        );
      }
    }

    public static class DoFnOfRoundEffectsProbability
      extends DoFn<KV<String, List<Double>>, KV<String, Double>> {

      private int roundDigitPosition;

      public DoFnOfRoundEffectsProbability(int digit) {
        this.roundDigitPosition = digit;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, List<Double>> e = c.element();

        // nothing to analyse
        if (e.getValue().size() == 0) {
          c.output(KV.of(e.getKey(), -1d));
          return;
        }

        int n = e
          .getValue()
          .stream()
          .parallel()
          .<Integer>map(d ->
            (
                Helper.DataTypes.lastDigit(d) != 0 &&
                Helper.DataTypes.lastDigit(
                  Helper.DataTypes.round(d, this.roundDigitPosition)
                ) ==
                0
              )
              ? 1
              : 0
          )
          .reduce(0, Integer::sum);

        int m = e.getValue().size();

        double nn = Double.parseDouble(Integer.toString(n));
        double mm = Double.parseDouble(Integer.toString(m));

        c.output(KV.of(e.getKey(), nn / mm));
      }
    }

    public static class DoFnOfTickChangeEvents
      extends DoFn<KV<String, List<KV<Integer, Event>>>, KV<String, List<Double>>> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, List<KV<Integer, Event>>> stockEvents = c.element();

        c.output(
          KV.of(
            stockEvents.getKey(),
            // detect tick changes
            compressTickChangedEvents(
              // local processing for light weight arrays object
              stockEvents
                .getValue()
                .stream()
                .parallel()
                .map(kv -> kv.getValue())
                .collect(Collectors.toList())
            )
          )
        );
      }
    }
  }

  public static class Helper {

    public static class DataTypes {

      public static Integer lastDigit(Double value) {
        return value.intValue() % 10;
      }

      public static int lastDigit(double value) {
        int result = ((int) value) % 10;

        return result;
      }

      public static Integer round(Double value, int digit) {
        value = value / Math.pow(10d, digit);
        return (int) (long) (Math.round(value) * Math.pow(10d, digit));
      }

      public static Integer LongToInt(Long l) {
        return Integer.parseInt(l.toString());
      }

      public static Integer DoubleToInt(Double d) {
        return d.intValue();
      }

      public static Double IntToDouble(Integer n) {
        return Double.parseDouble(n.toString());
      }

      public static <T> List<T> iterableToList(Iterable<T> o) {
        return StreamSupport
          .stream(((Iterable<T>) o).spliterator(), true)
          .parallel()
          .collect(Collectors.toList());
      }

      public static <T> Iterable<T> listToIterable(List<T> o) {
        return new Iterable<T>() {
          @Override
          public Iterator<T> iterator() {
            return o.iterator();
          }
        };
      }
    }

    public static <T> void AssertIsNotEmpty(
      String logTag,
      PCollection<T> values,
      SerializableFunction<T, List<?>> functionOfToList,
      boolean enabled
    ) {
      if (!enabled) return;

      values
        .apply(
          App.Helper.randomCallerTag(),
          MapElements
            .into(TypeDescriptors.integers())
            .via((T o) -> {
              if (functionOfToList == null) return 1;
              List some = functionOfToList.apply(o);
              if (some.isEmpty()) {
                log.info(
                  String.format("%s -> isEmpty: %s", logTag, o.toString())
                );
                assertTrue(!some.isEmpty());
              }
              return 1;
            })
        )
        .apply(App.Helper.randomCallerTag(), Sum.integersGlobally())
        .apply(
          App.Helper.randomCallerTag(),
          MapElements
            .into(TypeDescriptors.nulls())
            .via(i -> {
              assertTrue(i != 0);
              return null;
            })
        );
    }

    private static final ObjectMapper om = new ObjectMapper();

    /** toJson as method */
    public static String toJson(Object o) {
      try {
        return om.writeValueAsString(o);
      } catch (JsonProcessingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return "\n";
      }
    }

    /** ToJson PTransform */
    public static MapElements<Object, String> ToJson() {
      return MapElements
        .into(TypeDescriptors.strings())
        .via((Object o) -> {
          String output = "\n";

          try {
            output = om.writeValueAsString(o);

            log.info(output);

            if (DEBUG) log.info(output);
          } catch (JsonProcessingException e) {
            e.printStackTrace();
          }

          return output;
        });
    }

    public static MapElements LogPTransform() {
      return MapElements
        .into(TypeDescriptors.strings())
        .via((String s) -> {
          String msg = String.format("[DEBUG]: %s", s);
          if (DEBUG) log.info(msg);
          return s;
        });
    }

    public static <T> MapElements LogPTransform(
      SerializableFunction<T, String> toString
    ) {
      SerializableFunction<T, String> processor = (T o) -> {
        String s = toString.apply(o);
        if (DEBUG) log.info(s);
        return s;
      };

      return MapElements.into(TypeDescriptors.strings()).via(processor);
    }

    public static String getObjectIdMarker(Object o, String prefix) {
      return (
        getCallerFunctionName(3) +
        ":" +
        prefix +
        "@" +
        System.identityHashCode(o)
      );
    }

    public static String randomCallerTag(String... prefix) {
      String suffix = new Double(Math.random() * 1000).toString();

      String callerFunctionName = App.Helper.getCallerFunctionName(3);

      suffix = callerFunctionName + "-" + suffix;

      return prefix.length != 0 ? prefix[0] + "-" + suffix : suffix;
    }

    public static String getCallerFunctionName(int layer) {
      // get function caller and its class
      String callerMethodName = Thread
        .currentThread()
        .getStackTrace()[layer].getMethodName();

      return callerMethodName;
    }

    // @depreciated: parse multiline string
    public static String S() {
      StackTraceElement element = new RuntimeException().getStackTrace()[1];
      String name = element.getClassName().replace('.', '/') + ".java";
      StringBuilder sb = new StringBuilder();
      String line = null;

      // get function caller and its class
      String callerClassName = Thread
        .currentThread()
        .getStackTrace()[2].getClassName();

      log.info(String.format("ClassName: %s", callerClassName));

      // InputStream in = Class.forName(callerClassName).getClassLoader().getResourceAsStream(name);

      // assert in != null;

      // String s = convertStreamToString(in, element.getLineNumber());

      // return s.substring(s.indexOf("/*") + 2, s.indexOf("*/"));

      return "";
    }

    // From http://www.kodejava.org/examples/266.html
    private static String convertStreamToString(InputStream is, int lineNum) {
      /*
       * To convert the InputStream to String we use the BufferedReader.readLine() method. We iterate until the
       * BufferedReader return null which means there's no more data to read. Each line will appended to a
       * StringBuilder and returned as String.
       */
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));

      StringBuilder sb = new StringBuilder();

      String line = null;

      int i = 1;

      try {
        while ((line = reader.readLine()) != null) {
          if (i++ >= lineNum) {
            sb.append(line + "\n");
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          is.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      return sb.toString();
    }
  }

  public static Double max(List<Double> values) {
    assertTrue(!values.isEmpty());

    Double RESULT = null;

    RESULT = Collections.max(values);

    return RESULT;
  }

  public static Double median(Iterable<Double> doubles) {
    assertTrue(Iterables.size(doubles) > 0);

    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) {
      log.info(TAG + " # args");
      log.info(doubles.toString());
    }

    Double RESULT = null;

    List<Double> o = parallelSort(doubles);

    if (DEBUG) {
      log.info(TAG + " # sorted");
      log.info(o.toString());
    }

    // total count
    int count = o.size();

    if (count % 2 == 0) {
      RESULT = (o.get(count / 2 - 1) + o.get(count / 2)) / 2;
    } else {
      RESULT = o.get(count / 2);
    }

    return RESULT;
  }

  public static <T> List<KV<Integer, T>> sortKVList(
    Iterable<KV<Integer, T>> iterableKVs
  ) {
    assertTrue(Iterables.size(iterableKVs) > 0);

    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) {
      log.info(TAG + " # args");
      log.info(iterableKVs.toString());
    }

    List<KV<Integer, T>> RESULT = null;

    try {
      Map<Integer, T> cache = new HashMap<Integer, T>();

      for (KV<Integer, T> oo : iterableKVs) cache.put(
        oo.getKey(),
        oo.getValue()
      );

      final Iterable<Integer> it = (() -> cache.keySet().iterator());

      List<Integer> sortedTimeKey = App
        .parallelSort(
          // convert back to double(s) key
          StreamSupport
            .stream(it.spliterator(), true)
            .parallel()
            .map(o -> Double.parseDouble(o.toString()))
            .collect(Collectors.toList())
        )
        // convert back to int(s) key
        .stream()
        .parallel()
        .map(d -> d.intValue())
        .collect(Collectors.<Integer>toList());

      RESULT =
        sortedTimeKey
          .stream()
          .parallel()
          .map(key -> KV.of(key, cache.get(key)))
          .collect(Collectors.toList());

      assertTrue(!RESULT.isEmpty());
    } catch (Exception e) {
      if (DEBUG) {
        log.info(TAG + " # errors");
        log.info(e.toString());
      }

      e.printStackTrace();

      throw e;
    } finally {
      if (DEBUG) {
        log.info(TAG + " # results");
        log.info(RESULT.toString());
      }

      return RESULT;
    }
  }

  public static List<Double> parallelSort(Iterable<Double> iterableDoubles) {
    assertTrue(Iterables.size(iterableDoubles) > 0);

    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) {
      log.info(TAG + " # args");
      log.info(iterableDoubles.toString());
    }

    List<Double> RESULT = new ArrayList<Double>();

    try {
      List<Double> listDoubles = new ArrayList<Double>();

      iterableDoubles.forEach(RESULT::add);

      Double[] arrDoubles = new Double[RESULT.size()];

      for (int i = 0; i < RESULT.size(); i++) arrDoubles[i] = RESULT.get(i);

      Arrays.parallelSort(
        arrDoubles,
        new Comparator<Double>() {
          double ERROR_BOUND = Math.pow(10, -5);

          @Override
          public int compare(Double o1, Double o2) {
            double o = o1 - o2;

            return (o > ERROR_BOUND) ? 1 : (o < -ERROR_BOUND ? -1 : 0);
          }

          @Override
          public boolean equals(Object obj) {
            return super.equals(obj);
          }
        }
      );

      RESULT = Arrays.stream(arrDoubles).collect(Collectors.toList());

      assertTrue(!RESULT.isEmpty());
    } catch (Exception e) {
      if (DEBUG) {
        log.info("#errors");
        log.info(e.toString());
      }

      e.printStackTrace();

      throw e;
    } finally {
      if (DEBUG) {
        log.info("# results");
        log.info(RESULT.toString());
      }

      return RESULT;
    }
  }

  public static Double mean(List<Double> values) {
    assertTrue(!values.isEmpty());

    Double RESULT = 0d;

    Double count = Double.valueOf(values.size());

    Double sum = values
      .stream()
      .parallel()
      .map((Double d) -> d.doubleValue())
      .reduce(
        0d,
        (o, d) -> {
          return o + d;
        }
      );

    RESULT = sum / count;

    return RESULT;
  }

  public static List<Double> diffInDays(List<Double> values) {

    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) log.info(
      String.format("%s %s %s", TAG, "# args:", values.toString())
    );

    List<Double> RESULT = new ArrayList<Double>();

    try {

      String lastDay = null; 
      Double lastSeconds = null;

      for (Double d : values) {

        // split to days and seconds
        String value = String.valueOf((int) Math.rint(d.doubleValue()));
        String day = value.substring(0, 3);
        Double seconds = Double.parseDouble(value.substring(3));

        if (lastSeconds == null) {

          // init
          lastDay = day;
          lastSeconds = seconds;

        } else {

          if (day.equals(lastDay)) {
            
            Double c = (Math.abs(seconds - lastSeconds));

            lastSeconds = seconds;

            RESULT.add(c);

          }else{

            lastDay = day;

            lastSeconds = seconds;

          }

        }
      }
    } catch (Exception e) {

      if (DEBUG) log.info(
        String.format("%s %s %s", TAG, "# err:", e.toString())
      );

      e.printStackTrace();

      throw e;
    } finally {

      if (DEBUG) log.info(
        String.format("%s %s %s", TAG, "# results:", RESULT.toString())
      );

      return RESULT;
    }
  }

  // TODO: @deprecated
  public static List<Double> diff(List<Double> values) {
    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) log.info(
      String.format("%s %s %s", TAG, "# args:", values.toString())
    );

    List<Double> RESULT = new ArrayList<Double>();

    try {
      Double lastValue = null;

      for (Double d : values) {
        if (lastValue == null) {
          lastValue = d;
        } else {
          Double c = (Math.abs(lastValue - d));
          RESULT.add(c);
          lastValue = d;
        }
      }
    } catch (Exception e) {
      if (DEBUG) log.info(
        String.format("%s %s %s", TAG, "# err:", e.toString())
      );

      e.printStackTrace();

      throw e;
    } finally {
      if (DEBUG) log.info(
        String.format("%s %s %s", TAG, "# results:", RESULT.toString())
      );

      // assertTrue(RESULT.size() > 0);

      return RESULT;
    }
  }

  public static PCollection<KV<String, List<KV<Integer, Event>>>> filterValidNonAuctionTradingData(
    PCollection<KV<String, List<KV<Integer, Event>>>> eventData
  ) {
    App.Helper.AssertIsNotEmpty(
      " @args: ",
      eventData,
      kv -> kv.getValue(),
      false
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      eventData.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<KV<Integer, Event>>>> results = eventData.apply(
      App.Helper.randomCallerTag(),
      MapElements.via(
        new MapElementsTransforms.SimpleFunctionOfFilterNonValidTradingData()
      )
    );
    // .setCoder(
    // KvCoder.of(
    // StringUtf8Coder.of(),
    // ListCoder.of(KvCoder.of(TextualIntegerCoder.of(), Event.CODER))
    // )
    // );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static <T> PCollection<KV<String, List<KV<Integer, T>>>> groupAndSort(
    PCollection<KV<String, KV<Integer, T>>> eventData
  ) {
    String TAG = App.Helper.randomCallerTag();

    App.Helper.AssertIsNotEmpty(TAG + " @args: ", eventData, null, false);

    if (DEBUG) {
      eventData.apply(
        TAG,
        App.Helper.<KV<String, KV<Integer, T>>>LogPTransform(kv ->
          TAG + " #args " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<KV<Integer, T>>>> results = eventData
      .apply(
        App.Helper.randomCallerTag(),
        GroupByKey.<String, KV<Integer, T>>create()
      )
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(new MapElementsTransforms.SimpleFunctionOfKvSort<T>())
      );

    if (DEBUG) {
      String TAG_0 = App.Helper.randomCallerTag();

      results.apply(
        TAG_0,
        App.Helper.<KV<String, List<KV<Integer, T>>>>LogPTransform(kv ->
          TAG + " #results " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      TAG + "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static PCollection<ReportResult> buildReport(
    @NonNull PCollection<KV<String, ReportResult>> meanTradingTime,
    @NonNull PCollection<KV<String, ReportResult>> medianTradingTime,
    @NonNull PCollection<KV<String, ReportResult>> longestTradingTime,
    @NonNull PCollection<KV<String, ReportResult>> meanTickChangesTime,
    @NonNull PCollection<KV<String, ReportResult>> medianTickChangesTime,
    @NonNull PCollection<KV<String, ReportResult>> longestTickChangesTime,
    @NonNull PCollection<KV<String, ReportResult>> meanBidAskSpread,
    @NonNull PCollection<KV<String, ReportResult>> medianBidAskSpread,
    @NonNull PCollection<KV<String, ReportResult>> roundedTradedPriceBeZeroProb,
    @NonNull PCollection<KV<String, ReportResult>> roundedTradedVolumeBeZeroProb
  ) {
    /** debug */
    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      Arrays
        .asList(
          meanTradingTime,
          medianTradingTime,
          longestTradingTime,
          meanTickChangesTime,
          medianTickChangesTime,
          longestTickChangesTime,
          meanBidAskSpread,
          medianBidAskSpread,
          roundedTradedPriceBeZeroProb,
          roundedTradedVolumeBeZeroProb
        )
        .forEach(pcol ->
          pcol.apply(
            TAG,
            App.Helper.<KV<String, ReportResult>>LogPTransform(kv ->
              TAG + " # args: " + kv.toString()
            )
          )
        );
    }

    Coder<KV<String, ReportResult>> kvStockReportCoder = KvCoder.of(
      StringUtf8Coder.of(),
      ReportResult.CODER
    );

    PCollection<KV<String, ReportResult>> reportResults = PCollectionList
      .of(meanTradingTime)
      .and(medianTradingTime)
      .and(longestTradingTime)
      .and(meanTickChangesTime)
      .and(medianTickChangesTime)
      .and(longestTickChangesTime)
      .and(meanBidAskSpread)
      .and(medianBidAskSpread)
      .and(roundedTradedPriceBeZeroProb)
      .and(roundedTradedVolumeBeZeroProb)
      // merge all results to final results
      .apply(Flatten.<KV<String, ReportResult>>pCollections());
    // .setCoder(kvStockReportCoder);

    PCollection<ReportResult> results = reportResults
      .apply(
        App.Helper.randomCallerTag(),
        GroupByKey.<String, ReportResult>create()
      )
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(
          new MapElementsTransforms.SimpleFunctionOfBuildingReport()
        )
      );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<ReportResult>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(null, results, null, false);

    return results;
  }

  public static List<Double> compressTickChangedEvents(List<Event> listEvent) {
    // assertTrue(!listEvent.isEmpty());

    String TAG = App.Helper.randomCallerTag();

    if (DEBUG) {
      log.info(TAG + " # args " + listEvent.toString());
    }

    // local processing for light weight arrays object

    List<Double> p = new ArrayList<Double>() {};

    double lastTradePrice = -1;

    for (Event e : listEvent) {
      double price = e.getTradePrice();
      double time = (double) e.getDateTime();

      if (lastTradePrice == -1) {
        lastTradePrice = price;
        p.add(time);
      }

      if (lastTradePrice != price) {
        lastTradePrice = price;
        p.add(time);
      }
    }

    // assertTrue(!p.isEmpty());

    return diffInDays(p);
  }

  public static PCollection<KV<String, List<Double>>> ticksChangedTimes(
    PCollection<KV<String, List<KV<Integer, Event>>>> events
  ) {
    App.Helper.AssertIsNotEmpty(null, events, kv -> kv.getValue(), false);

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          String.format("%s %s %s", TAG, " # args :", kv.toString())
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = events
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.DoFnOfTickChangeEvents())
      )
      .setCoder(
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(DoubleCoder.of()))
      );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          String.format("%s %s %s", TAG, " # results :", kv.toString())
        )
      );
    }

    App.Helper.AssertIsNotEmpty(null, results, kv -> kv.getValue(), false);

    return results;
  }

  public static PCollection<KV<String, List<Double>>> tradedTimes(
    PCollection<KV<String, List<KV<Integer, Event>>>> events
  ) {
    App.Helper.AssertIsNotEmpty("@args: ", events, kv -> kv.getValue(), false);

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # args:" + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = trades(
      events,
      Event.Key.Time
    );

    results = results.apply(
      MapElements
      .into(new TypeDescriptor<KV<String, List<Double>>>() {})
      .via(kv -> KV.of(kv.getKey(), diffInDays(kv.getValue())))
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static PCollection<KV<String, List<Double>>> tradedVolumes(
    PCollection<KV<String, List<KV<Integer, Event>>>> events
  ) {
    App.Helper.AssertIsNotEmpty(
      "tradedVolumes: @args: ",
      events,
      kv -> kv.getValue(),
      false
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + "# args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = trades(
      events,
      Event.Key.TradeVolume
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static PCollection<KV<String, List<Double>>> tradedPrices(
    PCollection<KV<String, List<KV<Integer, Event>>>> events
  ) {
    App.Helper.AssertIsNotEmpty("@args: ", events, kv -> kv.getValue(), false);

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = trades(
      events,
      Event.Key.TradePrice
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static PCollection<KV<String, List<Double>>> trades(
    PCollection<KV<String, List<KV<Integer, Event>>>> events,
    Event.Key tradeEventProperty
  ) {
    App.Helper.AssertIsNotEmpty("@args: ", events, kv -> kv.getValue(), false);

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = events
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(
          new MapElementsTransforms.SimpleFunctionOfTrades(tradeEventProperty)
        )
      )
      .setCoder(
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(DoubleCoder.of()))
      );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  /** round effects on traded values and vol. of prob. of last digits becoming 0 */
  public static PCollection<KV<String, Double>> roundEffectsProbability(
    PCollection<KV<String, List<Double>>> stockNumericalEvents,
    int roundDigit
  ) {
    App.Helper.AssertIsNotEmpty(
      "@args: ",
      stockNumericalEvents,
      kv -> kv.getValue(),
      false
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      stockNumericalEvents.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          TAG + " # args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, Double>> results = stockNumericalEvents
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.DoFnOfRoundEffectsProbability(roundDigit))
      )
      .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, Double>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    return results;
  }

  public static PCollection<KV<String, List<Double>>> bidAskSpreads(
    PCollection<KV<String, List<KV<Integer, Event>>>> events
  ) {
    App.Helper.AssertIsNotEmpty(
      App.Helper.randomCallerTag() + "@args: ",
      events,
      kv -> kv.getValue(),
      false
    );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      events.apply(
        TAG,
        App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform(kv ->
          TAG + " # args: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, List<Double>>> results = events
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(
          new MapElementsTransforms.SimpleFunctionOfBidAskSpreads()
        )
      )
      .setCoder(
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(DoubleCoder.of()))
      );

    if (DEBUG) {
      String TAG = App.Helper.randomCallerTag();

      results.apply(
        TAG,
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          TAG + " # results: " + kv.toString()
        )
      );
    }

    App.Helper.AssertIsNotEmpty(
      App.Helper.randomCallerTag() + "@results: ",
      results,
      kv -> kv.getValue(),
      false
    );

    return results;
  }

  public static PCollection<KV<String, ReportResult>> computeReportResult(
    @NonNull String computationTag,
    @NonNull SerializableBiFunction<ReportResult, Double, ReportResult> attachEventResult,
    @NonNull PCollection<KV<String, Double>> domainValues
  ) {
    App.Helper.AssertIsNotEmpty(
      computationTag + "@args: ",
      domainValues,
      null,
      false
    );

    if (DEBUG) {
      domainValues.apply(
        App.Helper.randomCallerTag(),
        App.Helper.<KV<String, Double>>LogPTransform(kv ->
          computationTag + ":" + " # domainValues: " + kv.toString()
        )
      );
    }

    PCollection<KV<String, ReportResult>> results = domainValues
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(
          new MapElementsTransforms.SimpleFunctionOfComputeReportResult(
            attachEventResult
          )
        )
      )
      .setCoder(KvCoder.of(StringUtf8Coder.of(), ReportResult.CODER));

    if (DEBUG) {
      results.apply(
        App.Helper.randomCallerTag(),
        App.Helper.<KV<String, ReportResult>>LogPTransform(kv ->
          computationTag + ":" + " # reportResults: " + kv.toString()
        )
      );
    }

    return results;
  }

  public static PCollection<KV<String, ReportResult>> computeReportResultWithSideComputation(
    @NonNull String computationTag,
    @NonNull SerializableBiFunction<ReportResult, Double, ReportResult> attachEventResult,
    @NonNull PCollection<KV<String, List<Double>>> domainValues,
    @NonNull SerializableFunction<List<Double>, Double> sideComputation
  ) {
    App.Helper.AssertIsNotEmpty(
      computationTag + " @args: ",
      domainValues,
      kv -> kv.getValue(),
      false
    );

    if (DEBUG) {
      domainValues.apply(
        App.Helper.randomCallerTag(),
        App.Helper.<KV<String, List<Double>>>LogPTransform(kv ->
          computationTag +
          " # domainValues: " +
          kv.getKey() +
          ":" +
          kv.getValue().toString()
        )
      );
    }

    // assertTrue(domainValues.apply(Count.globally()) > 0);

    PCollection<KV<String, ReportResult>> results = domainValues
      .apply(
        App.Helper.randomCallerTag(),
        MapElements.via(
          new MapElementsTransforms.SimpleFunctionOfComputeReportResultWithSideComputation(
            attachEventResult,
            sideComputation
          )
        )
      )
      .setCoder(KvCoder.of(StringUtf8Coder.of(), ReportResult.CODER));

    if (DEBUG) {
      results.apply(
        App.Helper.randomCallerTag(),
        App.Helper.<KV<String, ReportResult>>LogPTransform(kv ->
          computationTag +
          " # reportResults: " +
          kv.getKey() +
          ":" +
          kv.getValue().toString()
        )
      );
    }

    return results;
  }

  // register all necessary encoders for large scale serializable computation
  public static void registerBeamCoders() {
    CoderRegistry c = CoderRegistry.createDefault();

    c.registerCoderProvider(
      CoderProviders.forCoder(
        new TypeDescriptor<KV<String, KV<Integer, Event>>>() {},
        KvCoder.of(
          StringUtf8Coder.of(),
          KvCoder.of(TextualIntegerCoder.of(), Event.CODER)
        )
      )
    );

    c.registerCoderProvider(
      CoderProviders.forCoder(
        new TypeDescriptor<KV<String, List<KV<Integer, Event>>>>() {},
        KvCoder.of(
          StringUtf8Coder.of(),
          ListCoder.of(KvCoder.of(BigIntegerCoder.of(), Event.CODER))
        )
      )
    );

    c.registerCoderProvider(
      CoderProviders.forCoder(
        new TypeDescriptor<KV<String, List<Double>>>() {},
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(DoubleCoder.of()))
      )
    );

    c.registerCoderProvider(
      CoderProviders.forCoder(
        new TypeDescriptor<KV<String, Double>>() {},
        KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of())
      )
    );
  }

  public static void initDataTypeCoders() {
    Event.initCoder();
    ReportResult.initCoder();
  }

  private void testReadLargeFilePerformance() throws java.io.IOException {
    // long start = new Date().getTime();
    long start2 = System.currentTimeMillis();

    Stream<String> lines = Files.lines(Paths.get(this.inputFiles));

    lines.forEach(l -> {
      // log.info(l);
    });

    long end2 = System.currentTimeMillis();

    long time = end2 - start2;

    String result = "Elapsed Time in milli seconds: " + time;

    log.info(result);
  }

  @Value("inputFiles")
  private String inputFiles;

  public static final DockerImageName REDIS_IMAGE = DockerImageName.parse(
    "redis:3.0.2"
  );

  // TODO: @depreciated
  // public static GenericContainer redis = new GenericContainer<>(REDIS_IMAGE)
  // .withCreateContainerCmdModifier(cmd -> cmd.withName("test-containers-redis")) // .withMemory("1000")
  // .withLabel("test-containers", "redis")
  // .withExposedPorts(5432)
  // .withReuse(true);

  // public static JdbcDatabaseContainer pgsqlContainer = new PostgreSQLContainer<>()
  // .withDatabaseName("oem")
  // .withPassword("oem")
  // .withUsername("oem");
  // .withInitScript("./inputs/init.sql");

  // public static JdbcDatabaseContainer pgsqlContainer = new PostgreSQLContainer<>()
  // // .withCreateContainerCmdModifier(cmd -> cmd.withName("test-containers-" + suffix + "pgsql")) //
  // .withMemory("1000")
  // .withCreateContainerCmdModifier(cmd -> cmd.withName("test-containers-pgsql-" +
  // RandomStringUtils.randomAlphanumeric(10))) // .withMemory("1000")
  // .withLabel("test-containers", "pgsql")
  // .withDatabaseName("oem")
  // .withPassword("oem")
  // .withUsername("oem")
  // .withExposedPorts(5432)
  // .withReuse(true);

  // public static JdbcDatabaseContainer c = new PostgreSQLContainerProvider()
  // .newInstance();
  // .withDatabaseName("db")
  // .withPassword("password")
  // .withUsername("username");

  // remarks: non blocking thread.
  private Thread threadOfCheckDataIsLoaded = null;

  public Thread checkDataIsLoaded() {
    if (threadOfCheckDataIsLoaded != null) return threadOfCheckDataIsLoaded;

    threadOfCheckDataIsLoaded =
      new Thread(() -> {
        JdbcTemplate sqlTemplate = initJdbcSQLTemplate();

        String tableName = "t_stockcodes";

        Integer i = 0;

        // check if the required tables are created
        while (i == 0) {
          i =
            sqlTemplate.queryForObject(
              "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
              new Object[] { tableName },
              Integer.class
            );

          try {
            Thread.sleep(5000);
          } catch (java.lang.InterruptedException e) {
            e.printStackTrace();
          }
        }

        log.info("data is loaded...");
      });

    return threadOfCheckDataIsLoaded;
  }

  private JdbcIO.DataSourceConfiguration dataSourceConfiguration = null;

  private JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
    if (this.dataSourceConfiguration != null) return dataSourceConfiguration;

    dataSourceConfiguration =
      JdbcIO.DataSourceConfiguration
        .create(
          "com.impossibl.postgres.jdbc.PGDriver",
          "jdbc:pgsql://oem:oem@127.0.0.1:5432/oem"
        )
        .withConnectionProperties(
          ValueProvider.StaticValueProvider
            .<String>of("jdbcUrl:jdbc:pgsql://oem:oem@127.0.0.1:5432/oem;")
            .<String>of("dataSource.user:oem;")
            .<String>of("dataSource.password:oem;")
            // .<String>of("dataSource.databaseName:oem;")
            // .<String>of("dataSource.port:5432;")
            .<String>of("connectionTimeout:100000;")
            .<String>of("idleTimeout:100000")
            .<String>of("maximumPoolSize:0") //200
        );

    return dataSourceConfiguration;
  }

  private DataSource dataSource = null;

  private DataSource getDataSource() {
    if (dataSource != null) return dataSource;

    Properties config = new Properties();

    // TODO: later set via external config
    config.setProperty("jdbcUrl", "jdbc:pgsql://oem:oem@127.0.0.1:5432/oem");

    config.setProperty("dataSource.user", "oem");
    config.setProperty("dataSource.password", "oem");
    config.setProperty("dataSource.databaseName", "oem");
    config.setProperty("dataSource.port", "5432");

    config.setProperty("connectionTimeout", "100000");
    config.setProperty("idleTimeout", "100000");
    config.setProperty("maximumPoolSize", "100");
    // config.setProperty("minimumIdle", "1000");

    // sql connection pool
    HikariConfig cfg = new HikariConfig(config);

    HikariDataSource dataSource = new HikariDataSource(cfg);

    dataSource.setPassword("oem");

    dataSource.setUsername("oem");

    this.dataSource = dataSource;

    return dataSource;
  }

  public JdbcTemplate initJdbcSQLTemplate() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate();

    jdbcTemplate.setDataSource(getDataSource());

    return jdbcTemplate;
  }

  @PostConstruct
  public void afterInitialize() throws Exception {
    log.info(App.Helper.randomCallerTag());

    this.start();
  }

  private static String[] args;

  public void start() throws Exception {
    log.info(App.Helper.randomCallerTag());

    assertTrue(this.inputFiles != null);

    log.info(this.inputFiles);

    // check if the database has been init with data.
    checkDataIsLoaded().start();

    initDataTypeCoders();

    PipelineOptions _options = PipelineOptionsFactory
      .fromArgs(App.args)
      .withValidation()
      .create();

    Pipeline p = Pipeline.create(_options);

    PCollection<String> stockCodes = p.apply(
      App.Helper.randomCallerTag(JdbcIO.Read.class.toString()),
      JdbcIO
        .<String>read()
        .withDataSourceConfiguration(getDataSourceConfiguration())
        .withQuery("SELECT * from T_stockcodes;")
        .withRowMapper((java.sql.ResultSet resultSet) -> resultSet.getString(1))
        .withCoder(StringUtf8Coder.of())
    );

    // group and sort stocks
    PCollection<KV<String, List<KV<Integer, Event>>>> sortedEvents = stockCodes
      .apply(
        App.Helper.randomCallerTag(JdbcIO.Write.class.toString()),
        JdbcIO
          .<String>write()
          .withDataSourceConfiguration(getDataSourceConfiguration())
          .withStatement(
            "SELECT * FROM T_events WHERE stock_code = ? ORDER BY date, time ASC;"
          )
          // .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
          .withPreparedStatementSetter((value, query) -> {
            query.setString(1, value);
          })
          .<EventListJdbcResult>withWriteResults((java.sql.ResultSet rs) -> {
            rs.beforeFirst();

            List<Event> events = new ArrayList<Event>();

            Integer id = Integer.MAX_VALUE;

            // log.info(String.format("id size: %d", id));

            while (rs.next()) {
              
              id -= 1;

              String eventString = "";

              for (var j = 1; j < 16; j++) {
                if (j == 1) {
                  eventString = rs.getString(1);
                  continue;
                }
                eventString += "," + rs.getString(j);
              }

              eventString += ",";

              events.add(Event.parseFromSourceLineString(eventString, id));

            }

            log.info(
              String.format(
                "data is fetched with stockcode = %s",
                events.get(0).getStockCode()
              )
            );

            return EventListJdbcResult.builder().values(events).build();
          })
          .withRetryConfiguration(
            JdbcIO.RetryConfiguration.create(
              1000,
              Duration.standardSeconds(300),
              Duration.standardSeconds(3)
            )
          )
          .withRetryStrategy(exception -> true)
      )
      .setCoder(EventListJdbcResult.GetCoder(Event.CODER))
      .apply(
        App.Helper.randomCallerTag(MapElements.class.toString()),
        MapElements
          .into(new TypeDescriptor<KV<String, List<KV<Integer, Event>>>>() {})
          .via((EventListJdbcResult result) -> {
            List<Event> events = result.getValues();

            if (events.isEmpty()) return KV.of("", null);

            List<KV<Integer, Event>> kvEvents = events
              .stream()
              .map(e -> KV.of(e.getDateTime(), e))
              .collect(Collectors.toList());

            return KV.of(events.get(0).getStockCode(), kvEvents);
          })
      )
      .setCoder(
        KvCoder.of(
          StringUtf8Coder.of(),
          ListCoder.of(KvCoder.of(TextualIntegerCoder.of(), Event.CODER))
        )
      );

    // valid data filter
    PCollection<KV<String, List<KV<Integer, Event>>>> cleanedEvents = filterValidNonAuctionTradingData(
      sortedEvents
    );

    // trade times
    PCollection<KV<String, List<Double>>> tradedTimes = tradedTimes(
      cleanedEvents
    );

    // trade prices
    PCollection<KV<String, List<Double>>> tradedPrices = tradedPrices(
      cleanedEvents
    );

    // trade volumes
    PCollection<KV<String, List<Double>>> tradedVolumes = tradedVolumes(
      cleanedEvents
    );

    // tick changes
    PCollection<KV<String, List<Double>>> ticksChangesTimes = ticksChangedTimes(
      cleanedEvents
    );

    // bid ask spread
    PCollection<KV<String, List<Double>>> bidAskSpreads = bidAskSpreads(
      cleanedEvents
    );

    // round effect and get last digit = 0 prob.
    PCollection<KV<String, Double>> roundedTradedPriceLastDigitBeZeroProb = roundEffectsProbability(
      tradedPrices,
      0
    );

    PCollection<KV<String, Double>> roundedTradedVolumeLastDigitBeZeroProb = roundEffectsProbability(
      tradedVolumes,
      0
    );

    buildReport(
      computeReportResultWithSideComputation(
        "setMeanTradingTime",
        (ReportResult result, Double d) -> result.setMeanTradingTime(d),
        tradedTimes,
        App::mean
      ),
      computeReportResultWithSideComputation(
        "setMedianTradingTime",
        (ReportResult result, Double d) -> result.setMedianTradingTime(d),
        tradedTimes,
        App::median
      ),
      computeReportResultWithSideComputation(
        "setLongestTradingTime",
        (ReportResult result, Double d) -> result.setLongestTradingTime(d),
        tradedTimes,
        App::max
      ),
      computeReportResultWithSideComputation(
        "setMeanTickChangesTime",
        (ReportResult result, Double d) -> result.setMeanTickChangesTime(d),
        ticksChangesTimes,
        App::mean
      ),
      computeReportResultWithSideComputation(
        "setMedianTickChangesTime",
        (ReportResult result, Double d) -> result.setMedianTickChangesTime(d),
        ticksChangesTimes,
        App::median
      ),
      computeReportResultWithSideComputation(
        "setLongestTickChangesTime",
        (ReportResult result, Double d) -> result.setLongestTickChangesTime(d),
        ticksChangesTimes,
        App::max
      ),
      computeReportResultWithSideComputation(
        "setMeanBidAskSpreads",
        (ReportResult result, Double d) -> result.setMeanBidAskSpreads(d),
        bidAskSpreads,
        App::mean
      ),
      computeReportResultWithSideComputation(
        "setMedianBidAskSpreads",
        (ReportResult result, Double d) -> result.setMedianBidAskSpreads(d),
        bidAskSpreads,
        App::median
      ),
      computeReportResult(
        "setRoundedTradedPriceBeZeroProb",
        (ReportResult result, Double d) ->
          result.setRoundedTradedPriceBeZeroProb(d),
        roundedTradedPriceLastDigitBeZeroProb
      ),
      computeReportResult(
        "setRoundedTradedVolumeBeZeroProb",
        (ReportResult result, Double d) ->
          result.setRoundedTradedVolumeBeZeroProb(d),
        roundedTradedVolumeLastDigitBeZeroProb
      )
    )
      .apply(ToJson.<ReportResult>of())
      .apply(TextIO.write().to("./outputs/QuantStockReports.json"));

    // TODO: DEBUG
    // sortedEvents
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("/tmp/outputs/sorted.txt"));

    // TODO: DEBUG
    // cleanedEvents
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("/tmp/outputs/cleaned.txt"));

    // TODO: DEBUG
    // tradedTimes
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("/tmp/outputs/times.txt"));

    // TODO: DEBUG
    // tradedPrices
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/prices.txt"));

    // TODO: DEBUG
    // tradedVolumes
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/volumes.txt"));

    // TODO: DEBUG
    // ticksChangesTimes
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/tickchanges.txt"));

    // TODO: DEBUG
    // bidAskSpreads
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/bidaskspreads.txt"));

    // TODO: DEBUG
    // roundedTradedPriceLastDigitBeZeroProb
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/zeroProbPrice.txt"));

    // TODO: DEBUG
    // roundedTradedVolumeLastDigitBeZeroProb
    //   .apply(MapElements.into(TypeDescriptors.strings()).via(e -> e.toString()))
    //   .apply(TextIO.write().to("./outputs/zeroProbVolume.txt"));

    // wait for tables are created
    checkDataIsLoaded().join();

    p.run().waitUntilFinish();

    log.info("success");
  }

  public static void main(String[] args) throws Exception {
    log.info(App.Helper.randomCallerTag());

    App.args = args;

    new AnnotationConfigApplicationContext(
      App.class
    );
  }
}