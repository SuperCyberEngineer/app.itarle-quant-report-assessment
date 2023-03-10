package org.apache.beam.examples;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.MoreObjects;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcWriteResult;
// import org.ehcache.Cache;
// import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
// import org.apache.beam.sdk.extensions.sorter.SortValues;

import org.joda.time.Duration;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.apache.beam.sdk.io.TextIO;

//  SQLException
//  PreparedStatement

/** Unit test for Beam App */

// @Slf4j
// @Testcontainers
@RunWith(JUnit4.class)
public class AppTest {

  private static org.slf4j.Logger log = LoggerFactory.getLogger(AppTest.class);

  public static class Fixtures {

    public static Create.Values<Event> eventsCreationTransform;
    public static Create.Values<KV<String, List<KV<Integer, Event>>>> eventListTransform;
    public static Create.Values<KV<String, KV<Integer, Event>>> eventsTransform;
    public static Create.Values<KV<String, List<Double>>> eventDataListTransform;
  }

  @Before
  public void beforeAll() {
    System.setProperty("ENV", "dev");

    /** logging configs */
    // System.setProperty(
    // "java.util.logging.SimpleFormatter.format",
    // "[%1$tF %1$tT] [%4$-7s] %5$s %n"
    // );
    // log.setLevel(Level.ALL);

    LoggingConfig.configLogger();

    log = LoggerFactory.getLogger(AppTest.class);

    log.info(App.Helper.randomCallerTag());

    ReportResult.initCoder();
    Event.initCoder();

    Fixtures.eventsCreationTransform =
      Create.<Event>of(
        IntStream
          .range(0, 100)
          .boxed()
          .map(i ->
            Event
              .builder()
              .stockCode(new Integer(i + 1000).toString())
              .condition(" ")
              .bidPrice(999d)
              .askPrice(1999d)
              .tradePrice(1d)
              .build()
          )
          .collect(Collectors.toList())
      );

    Fixtures.eventDataListTransform =
      Create.<KV<String, List<Double>>>of(
        KV.of(
          "1000",
          IntStream
            .range(10, 100)
            .asDoubleStream()
            .map(d -> d / 10d)
            .boxed()
            .collect(Collectors.toList())
        ),
        KV.of(
          "1003",
          IntStream
            .range(30, 300)
            .asDoubleStream()
            .map(d -> d / 15d)
            .boxed()
            .collect(Collectors.toList())
        ),
        KV.of(
          "1005",
          IntStream
            .range(50, 500)
            .asDoubleStream()
            .map(d -> d / 20d)
            .boxed()
            .collect(Collectors.toList())
        )
      );

    Fixtures.eventsTransform =
      Create.<KV<String, KV<Integer, Event>>>of(
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition(" ")
                .bidPrice(999d)
                .askPrice(1999d)
                .tradePrice(1d)
                .build()
            )
          )
        ),
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition(" ")
                .bidPrice(1999d)
                .askPrice(999d)
                .tradePrice(1d)
                .build()
            )
          )
        ),
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition("XT")
                .bidPrice(1999d)
                .askPrice(999d)
                .tradePrice(2d)
                .build()
            )
          )
        ),
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition("XT")
                .bidPrice(999d)
                .askPrice(1999d)
                .tradePrice(2d)
                .build()
            )
          )
        ),
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition("@a")
                .bidPrice(1999d)
                .askPrice(999d)
                .tradePrice(3d)
                .build()
            )
          )
        ),
        (
          KV.of(
            "999",
            KV.of(
              0,
              Event
                .builder()
                .stockCode("1000")
                .condition("@a")
                .bidPrice(999d)
                .askPrice(1999d)
                .tradePrice(2d)
                .build()
            )
          )
        )
      );

    Fixtures.eventListTransform =
      Create.<KV<String, List<KV<Integer, Event>>>>of(
        KV.of(
          "1000",
          Arrays.<KV<Integer, Event>>asList(
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition(" ")
                .bidPrice(998d)
                .askPrice(1998d)
                .tradePrice(19.4d)
                .updateType(0)
                .dateTime(2)
                .build()
            ),
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition(" ")
                .bidPrice(1998d)
                .askPrice(998d)
                .tradePrice(39.5d)
                .updateType(1)
                .dateTime(5)
                .build()
            ),
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition("XT")
                .bidPrice(1998d)
                .askPrice(998d)
                .tradePrice(39.5d)
                .updateType(1)
                .dateTime(8)
                .build()
            ),
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition("XT")
                .bidPrice(998d)
                .askPrice(1998d)
                .tradePrice(59.7d)
                .updateType(1)
                .dateTime(10)
                .build()
            ),
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition("@a")
                .bidPrice(1998d)
                .askPrice(998d)
                .tradePrice(70.4d)
                .updateType(1)
                .dateTime(12)
                .build()
            ),
            KV.of(
              -1,
              Event
                .builder()
                .stockCode("1000")
                .condition("@a")
                .bidPrice(998d)
                .askPrice(1998d)
                .tradePrice(70.4d)
                .updateType(1)
                .dateTime(15)
                .build()
            )
          )
        )
      );
  }

  // TODO: for later use, default is ok
  public void testBeamRowSchemeEncoding() {}

  @Rule
  public final transient TestPipeline pipeline_testReportResultSchemaToJsonPTransform = TestPipeline.create();

  // test: to json string transform
  // TODO
  @Test
  @Category(ValidatesRunner.class)
  public void testReportResultSchemaToJsonPTransform() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testReportResultSchemaToJsonPTransform;

    PCollection<ReportResult> result = p.apply(
      App.Helper.randomCallerTag(),
      Create.of(
        ReportResult
          .builder()
          .stockCode("100")
          .meanTradingTime(100d)
          .medianTradingTime(100d)
          .meanTickChangesTime(100d)
          .build(),
        ReportResult
          .builder()
          .stockCode("101")
          .meanTradingTime(101d)
          .medianTradingTime(101d)
          .meanTickChangesTime(101d)
          .build(),
        ReportResult
          .builder()
          .stockCode("102")
          .meanTradingTime(102d)
          .medianTradingTime(102d)
          .meanTickChangesTime(102d)
          .build()
      )
    );

    assertTrue(result.hasSchema());

    PCollection<String> jsonStrs = result.apply(
      "[testReportResultSchemaToJsonPTransform]: 1",
      App.Helper.ToJson()
    );

    String s =
      "{\"stockCode\":\"101\",\"meanTradingTime\":101.0,\"meanTickChangesTime\":101.0,\"meanBidAskSpreads\":null,\"medianBidAskSpreads\":null,\"medianTradingTime\":101.0,\"medianTickChangesTime\":null,\"roundedTradedPriceBeZeroProb\":null,\"roundedTradedVolumeBeZeroProb\":null,\"longestTradingTime\":null,\"longestTickChangesTime\":null}\n" +
      "{\"stockCode\":\"100\",\"meanTradingTime\":100.0,\"meanTickChangesTime\":100.0,\"meanBidAskSpreads\":null,\"medianBidAskSpreads\":null,\"medianTradingTime\":100.0,\"medianTickChangesTime\":null,\"roundedTradedPriceBeZeroProb\":null,\"roundedTradedVolumeBeZeroProb\":null,\"longestTradingTime\":null,\"longestTickChangesTime\":null}\n" +
      "{\"stockCode\":\"102\",\"meanTradingTime\":102.0,\"meanTickChangesTime\":102.0,\"meanBidAskSpreads\":null,\"medianBidAskSpreads\":null,\"medianTradingTime\":102.0,\"medianTickChangesTime\":null,\"roundedTradedPriceBeZeroProb\":null,\"roundedTradedVolumeBeZeroProb\":null,\"longestTradingTime\":null,\"longestTickChangesTime\":null}";

    // """
    //
    // {"stockCode":"101","meanTradingTime":101.0,"meanTickChangesTime":100.0,"meanBidAskSpreads":null,"medianBidAskSpreads":null,"medianTradingTime":null,"medianTickChangesTime":100.0,"roundedTradedPriceBeZeroProb":100.0,"roundedTradedVolumeBeZeroProb":null,"longestTradingTime":null,"longestTickChangesTime":null}
    //
    // {"stockCode":"100","meanTradingTime":100.0,"meanTickChangesTime":100.0,"meanBidAskSpreads":null,"medianBidAskSpreads":null,"medianTradingTime":null,"medianTickChangesTime":100.0,"roundedTradedPriceBeZeroProb":100.0,"roundedTradedVolumeBeZeroProb":null,"longestTradingTime":null,"longestTickChangesTime":null}
    //
    // {"stockCode":"102","meanTradingTime":102.0,"meanTickChangesTime":100.0,"meanBidAskSpreads":null,"medianBidAskSpreads":null,"medianTradingTime":null,"medianTickChangesTime":100.0,"roundedTradedPriceBeZeroProb":100.0,"roundedTradedVolumeBeZeroProb":null,"longestTradingTime":null,"longestTickChangesTime":null}
    // """;

    List<String> splittedJsons = Arrays
      .asList(s.split("\n"))
      .stream()
      .parallel()
      .map(str -> str.trim())
      .collect(Collectors.toList());

    PAssert.that(jsonStrs).containsInAnyOrder(splittedJsons);

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testRoundEffectsProb = TestPipeline.create();

  @Test // done
  @Category(ValidatesRunner.class)
  public void testRoundEffectsProb() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testRoundEffectsProb;

    PCollection<KV<String, List<Double>>> oo = p.apply(
      "[testRoundEffectsProb: 0]",
      Fixtures.eventDataListTransform
    );

    PCollection<KV<String, Double>> o = App.roundEffectsProbability(oo, 0);

    PCollection<Double> ooo = o.apply(
      "[testRoundEffectsProb: 1]",
      Values.<Double>create()
    );

    // debug
    ooo.apply(
      "[testRoundEffectsProb: 2]",
      MapElements
        .into(TypeDescriptors.doubles())
        .via((Double d) -> {
          log.info(d.toString());
          assertTrue(d <= 0.1);
          assertTrue(d >= 0);
          return d;
        })
    );

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testMergePCollections = TestPipeline.create();

  @Test // done
  @Category(ValidatesRunner.class)
  public void testMergePCollections() {
    TestPipeline p = pipeline_testMergePCollections;

    PCollection<Integer> _ints_0 = p.apply("[0] Create", Create.of(1, 2, 3));

    PCollection<Integer> _ints_1 = p.apply("[1] Create", Create.of(4, 5, 6));

    PCollection<Integer> _ints_2 = p.apply("[2] Create", Create.of(7, 8, 9));

    PCollectionList<Integer> ints_3 = PCollectionList
      .of(_ints_0)
      .and(_ints_1)
      .and(_ints_2);

    PCollection<Integer> _ints_4 = ints_3.apply(
      Flatten.<Integer>pCollections()
    );

    PAssert.that(_ints_4).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9);

    p.run().waitUntilFinish();
  }

  @Test // done
  public void testLastDigit() {
    Double value = 123.4d;

    assertEquals(App.Helper.DataTypes.lastDigit(value).intValue(), 3);
  }

  @Test // done
  public void testRound() {
    Double value_0 = 123.5d;
    Double value_1 = 129.4d;
    Double value_2 = 139d;

    assertEquals(App.Helper.DataTypes.round(value_0, 0).intValue(), 124);

    assertEquals(App.Helper.DataTypes.round(value_1, 1).intValue(), 130);

    assertEquals(App.Helper.DataTypes.round(value_2, 1).intValue(), 140);
  }

  // @depreciated
  @FunctionalInterface
  public interface EventBuilder {
    public Event buildMap(final Event e, List<KV<Event.Key, String>> kvs);
  }

  // @depreciated
  EventBuilder o = (event, kvs) -> {
    kvs.stream().forEach(kv -> {});
    return event;
  };

  @Rule
  public final transient TestPipeline pipeline_testTicksChangedEvents = TestPipeline.create();

  // @Test
  @Category(ValidatesRunner.class)
  public void testTicksChangedEvents() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testTicksChangedEvents;

    PCollection<Double> times = App
      .ticksChangedTimes(
        p.apply(App.Helper.randomCallerTag(), Fixtures.eventListTransform)
      )
      .apply(App.Helper.randomCallerTag(), Values.<List<Double>>create())
      .apply(
        App.Helper.randomCallerTag(),
        FlatMapElements.via(new ParDoTransforms.FlatMapTicksChangedTimes())
      );

    PAssert.that(times).containsInAnyOrder(3d, 5d, 2d);

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testDataViewIndexing = TestPipeline.create();

  // TODO: not using view index currently
  // @Test
  public void testDataViewIndexing() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testDataViewIndexing;

    PCollection<Integer> ints = p.apply(
      Create.<Integer>of(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    );

    PCollectionView<List<Integer>> intsView = ints.apply(
      View.<Integer>asList()
    );

    PCollection<Integer> o = ints
      .apply(App.Helper.randomCallerTag(), Count.globally())
      .apply(
        MapElements
          .into(TypeDescriptors.lists(TypeDescriptors.integers()))
          .via((Long i) ->
            IntStream
              .range(0, App.Helper.DataTypes.LongToInt(i))
              .boxed()
              .collect(Collectors.toList())
          )
      )
      .apply(
        FlatMapElements.via( // .into(TypeDescriptors.integers())
          new InferableFunction<List<Integer>, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> apply(List<Integer> d) {
              return App.Helper.DataTypes.listToIterable(d);
            }
          }
        )
      )
      .apply(
        ParDo
          .of(
            new DoFn<Integer, Integer>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                List<Integer> intsViewResult = c.sideInput(intsView);
                int i = c.element();
                c.output(intsViewResult.get(i) * 2);
              }
            }
          )
          .withSideInputs(intsView)
      )
      .apply(
        MapElements
          .into(TypeDescriptors.integers())
          .via((Integer i) -> {
            log.info(i.toString());
            return i * 2;
          })
      );
  }

  @Test
  public void testFormatDoubleDateStringToIntegerString() {
    String intStr1 = Event.Helpers.formatDoubleDateStringToIntegerString(
      "11111111",
      "2.0"
    );

    assertEquals(intStr1, "1112");

    String intStr2 = Event.Helpers.formatDoubleDateStringToIntegerString(
      "2",
      "3"
    );

    assertEquals(intStr2, "23");
  }

  @Test
  public void shouldReturnDiff() {
    List<Double> values = new ArrayList<Double>() {
      {
        add(0d);
        add(1d);
        add(1d);
        add(3d);
        add(4d);
        add(10d);
      }
    };

    List<Double> o = App.diff(values);

    assertTrue(o.contains(0d));
    assertTrue(o.contains(1d));
    assertTrue(o.contains(2d));
    assertTrue(o.contains(6d));
  }

  @Test
  public void shouldDoParallelSort() {
    final List<Double> values = Arrays.asList(
      0d,
      1d,
      10d,
      1d,
      12d,
      3d,
      4d,
      10d
    );

    final List<Double> sortedValues = App.parallelSort(
      App.Helper.DataTypes.listToIterable(values)
    );

    final List<Double> answers = Arrays.asList(
      0d,
      1d,
      1d,
      3d,
      4d,
      10d,
      10d,
      12d
    );

    IntStream
      .range(0, answers.size())
      .parallel()
      .forEach(i -> {
        assertEquals(
          answers.get(i).doubleValue(),
          sortedValues.get(i).doubleValue(),
          1E-10
        );
      });
  }

  @Test
  public void shouldReturnMean() {
    List<Double> values = Arrays.asList(3d, 4d, 1d);

    assertEquals(App.mean(values).doubleValue(), 8d / 3, 1E-10);
  }

  @Test
  public void shouldReturnMedian() {
    // List<Double> values = Arrays.asList(0d, 2d, 3d, 5d, 2.5d, 2.5d);
    String some = "4.2029013E7, 4.2030294E7, 4.20322E7, 4.2046529E7, 4.2047015E7, 4.2047278E7, 4.2048665E7, 4.2049359E7, 4.2049819E7, 4.2050399E7, 4.2051374E7, 4.2052706E7, 4.2056162E7, 4.2132226E7, 4.2133118E7, 4.2133911E7, 4.2141838E7, 4.2143323E7, 4.2144396E7, 4.2144465E7, 4.2144605E7, 4.2145837E7, 4.2154738E7, 4.2157078E7, 4.2157598E7, 4.2158152E7, 4.2158238E7, 4.2158835E7, 4.2230912E7, 4.2231032E7, 4.2231055E7, 4.223445E7, 4.2234486E7, 4.2235251E7, 4.2239194E7, 4.2239916E7, 4.2242438E7, 4.2242951E7, 4.2248008E7, 4.224951E7, 4.2252518E7, 4.2253986E7, 4.2255767E7, 4.2255792E7, 4.2258122E7, 4.2258306E7, 4.2258328E7, 4.2258434E7, 4.2258665E7, 4.2332076E7, 4.2333636E7, 4.2333973E7, 4.2334109E7, 4.2335885E7, 4.2335957E7, 4.2336245E7, 4.2336557E7, 4.2337784E7, 4.2339232E7, 4.2339312E7, 4.2340017E7, 4.2342111E7, 4.2342232E7, 4.2342314E7, 4.2342319E7, 4.2342326E7, 4.2342338E7, 4.234234E7, 4.234234E7, 4.2342341E7, 4.2342344E7, 4.2342344E7, 4.2342345E7, 4.2342346E7, 4.234235E7, 4.234235E7, 4.2342366E7, 4.2342369E7, 4.2342369E7, 4.2342376E7, 4.2342376E7, 4.2342377E7, 4.234238E7, 4.2342386E7, 4.2342387E7, 4.2342405E7, 4.2342406E7, 4.2342433E7, 4.2342435E7, 4.2342442E7, 4.2342493E7, 4.2342493E7, 4.23425E7, 4.23425E7, 4.2342507E7, 4.2342515E7, 4.234252E7, 4.2342541E7, 4.2342542E7, 4.2342546E7, 4.2342561E7, 4.234259E7, 4.2342591E7, 4.2342618E7, 4.2342631E7, 4.2342644E7, 4.2342649E7, 4.2342671E7, 4.2342681E7, 4.2342684E7, 4.2342684E7, 4.2342696E7, 4.2342727E7, 4.2342742E7, 4.234278E7, 4.2342946E7, 4.2342948E7, 4.2342951E7, 4.2342953E7, 4.2342954E7, 4.2342966E7, 4.2342967E7, 4.2342971E7, 4.2342971E7, 4.2342975E7, 4.2342975E7, 4.2342978E7, 4.2342981E7, 4.2343018E7, 4.2343288E7, 4.2343371E7, 4.2343375E7, 4.2343493E7, 4.2343564E7, 4.2343565E7, 4.2343594E7, 4.2343601E7, 4.2343601E7, 4.2343639E7, 4.2343652E7, 4.2343664E7, 4.2343675E7, 4.2343689E7, 4.2343689E7, 4.2343831E7, 4.2343843E7, 4.234388E7, 4.2343921E7, 4.2343945E7, 4.2343991E7, 4.2344088E7, 4.2344089E7, 4.2344109E7, 4.2344198E7, 4.2344227E7, 4.2344228E7, 4.2344229E7, 4.2344269E7, 4.2344282E7, 4.2344315E7, 4.2344316E7, 4.2344335E7, 4.234448E7, 4.2344502E7, 4.2344521E7, 4.2344652E7, 4.2344666E7, 4.2345542E7, 4.2345806E7, 4.2345949E7, 4.2346216E7, 4.2346399E7, 4.2346547E7, 4.2346718E7, 4.2346755E7, 4.2347092E7, 4.2347147E7, 4.2347193E7, 4.2347223E7, 4.2347334E7, 4.234737E7, 4.2347379E7, 4.2347401E7, 4.2347406E7, 4.2347406E7, 4.2347437E7, 4.2347496E7, 4.2347523E7, 4.2347557E7, 4.234756E7, 4.2347579E7, 4.2347583E7, 4.2347599E7, 4.2347617E7, 4.2347698E7, 4.2347729E7, 4.2347729E7, 4.2347731E7, 4.2347757E7, 4.2347795E7, 4.2347814E7, 4.2347974E7, 4.2348066E7, 4.2348067E7, 4.2348069E7, 4.2348249E7, 4.2348546E7, 4.23486E7, 4.2348707E7, 4.2348742E7, 4.2348863E7, 4.2348869E7, 4.2348923E7, 4.234896E7, 4.2349243E7, 4.2349424E7, 4.234969E7, 4.23497E7, 4.2349702E7, 4.234972E7, 4.2349731E7, 4.2349741E7, 4.2349927E7, 4.2349937E7, 4.2349939E7, 4.2349978E7, 4.2350013E7, 4.2350014E7, 4.2350121E7, 4.2350245E7, 4.235032E7, 4.2350422E7, 4.2350504E7, 4.2350681E7, 4.2350705E7, 4.2350719E7, 4.2351111E7, 4.235148E7, 4.2351702E7, 4.2351771E7, 4.235236E7, 4.2352439E7, 4.2352476E7, 4.2352557E7, 4.2352573E7, 4.2352797E7, 4.2353138E7, 4.2353249E7, 4.2354003E7, 4.2354081E7, 4.2354451E7, 4.2354647E7, 4.2354745E7, 4.2354934E7, 4.2354985E7, 4.2355485E7, 4.2356052E7, 4.2356349E7, 4.2356408E7, 4.2356457E7, 4.2356519E7, 4.2356566E7, 4.2356587E7, 4.2356603E7, 4.2356631E7, 4.2356834E7, 4.2356889E7, 4.2357079E7, 4.2357114E7, 4.2357123E7, 4.2357141E7, 4.2357143E7, 4.2357147E7, 4.235725E7, 4.2357336E7, 4.2357389E7, 4.2357545E7, 4.2358111E7, 4.23582E7, 4.2358272E7, 4.2358418E7, 4.2358544E7, 4.2358584E7, 4.2358594E7, 4.2358833E7, 4.2358905E7, 4.2358944E7, 4.2358957E7, 4.2359027E7, 4.235908E7";
    List<String> someList = Arrays.asList(some.split(", "));
    List<Double> someDoubleList = someList.stream().map(s -> Double.parseDouble(s)).collect(Collectors.toList());

    Double o = App.median(someDoubleList);

    log.info(String.format("median: %f", o.doubleValue()));

    assertEquals(o.doubleValue(), 42343837, 1E-10);
    assert true;
  }

  @Test // passed
  public void shouldReturnMax() {
    log.info(App.Helper.randomCallerTag());

    List<Double> values = Arrays.asList(0d, 2d, 3d, 5d, 2.5d, 2.5d);

    Double o = App.max(values);

    log.info(String.format("max: %f", o.doubleValue()));

    assertEquals(o.doubleValue(), 5d, 1E-10);
  }

  @Rule
  public final transient TestPipeline pipeline_testFilterValidNonAuctionTradingData = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterValidNonAuctionTradingData() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testFilterValidNonAuctionTradingData;

    PCollection<KV<String, List<KV<Integer, Event>>>> o = p.apply(
      App.Helper.randomCallerTag(),
      Fixtures.eventListTransform
    );

    App
      .filterValidNonAuctionTradingData(o)
      // assertions
      .apply(
        ParDo.of(new ParDoTransforms.AssertFilterValidNonAuctionTradingData())
      );

    p.run().waitUntilFinish();
  }

  @Test // done
  public void testSortKVList() {
    log.info(App.Helper.randomCallerTag());

    Iterable<KV<Integer, Integer>> input = Arrays.asList(
      KV.of(3, 30),
      KV.of(2, 20),
      KV.of(4, 40),
      KV.of(5, 50)
    );

    List<KV<Integer, Integer>> result = App.<Integer>sortKVList(input);

    assertEquals(result.get(0).getKey().intValue(), 2);

    assertEquals(result.get(0).getValue().intValue(), 20);

    assertEquals(result.get(1).getKey().intValue(), 3);

    assertEquals(result.get(1).getValue().intValue(), 30);

    assertEquals(result.get(2).getKey().intValue(), 4);

    assertEquals(result.get(2).getValue().intValue(), 40);

    assertEquals(result.get(3).getKey().intValue(), 5);

    assertEquals(result.get(3).getValue().intValue(), 50);
  }

  @Test // done
  public void testReportResultMergeAndEquals() {
    log.info(App.Helper.randomCallerTag());

    ReportResult o1 = ReportResult
      .builder()
      .roundedTradedVolumeBeZeroProb(0.5d)
      .build();

    ReportResult o2 = ReportResult.builder().longestTradingTime(1000d).build();

    ReportResult o3 = ReportResult.builder().build();

    ReportResult o4 = ReportResult
      .builder()
      .roundedTradedVolumeBeZeroProb(0.5d)
      .longestTradingTime(1000d)
      .build();

    assertEquals(o3.addResult(o2).addResult(o1), o4);
  }

  public static class JdbcResults {

    @Accessors(chain = true)
    @Data
    @lombok.Builder
    public static class StockEvent extends JdbcWriteResult {

      public String value;
    }
  }

  public static class ParDoTransforms {

    public static class Random<T, M> extends DoFn<T, M> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        assert true;
      }
    }

    public static class AssertFilterValidNonAuctionTradingData
      extends DoFn<KV<String, List<KV<Integer, Event>>>, Void> {

      public void assertKvTrue(KV<String, List<KV<Integer, Event>>> o) {
        List<KV<Integer, Event>> oo = o.getValue();

        for (KV<Integer, Event> kv : oo) {
          double bid = kv.getValue().getBidPrice();

          double ask = kv.getValue().getAskPrice();

          String cond = kv.getValue().getCondition();

          assertTrue(bid < ask);

          assertTrue(cond.equals(" ") || cond.equals("XT"));
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        assertKvTrue(c.element());
      }
    }

    public static class FlatMapTicksChangedTimes
      extends InferableFunction<List<Double>, Iterable<Double>> {

      @Override
      public Iterable<Double> apply(List<Double> d) {
        return App.Helper.DataTypes.listToIterable(d);
      }
    }

    public static class AssertGroupAndSort
      extends DoFn<KV<String, List<KV<Integer, String>>>, Void> {

      public static void assertKvListEquals(
        List<KV<Integer, String>> o1,
        List<KV<Integer, String>> o2
      ) {
        for (int i = 0; i < o1.size(); i++) {
          assertEquals(
            o1.get(i).getKey().intValue(),
            o2.get(i).getKey().intValue()
          );

          assertEquals(o1.get(i).getValue(), o2.get(i).getValue());
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        // for case key = 1000
        if (c.element().getKey() == "1000") {
          List<KV<Integer, String>> o = new ArrayList<KV<Integer, String>>() {
            {
              add(KV.of(1, "1001"));
              add(KV.of(2, "1000"));
              add(KV.of(3, "1000"));
              add(KV.of(4, "1000"));
              add(KV.of(5, "1000"));
            }
          };

          List<KV<Integer, String>> oo = new ArrayList<KV<Integer, String>>() {};

          c.element().getValue().forEach(oo::add);

          assertKvListEquals(o, oo);
        }

        // for case key = 1005
        if (c.element().getKey() == "1005") {
          List<KV<Integer, String>> o = new ArrayList<KV<Integer, String>>() {
            {
              add(KV.of(1, "1005"));
              add(KV.of(2, "1005"));
              add(KV.of(3, "1005"));
              add(KV.of(4, "1005"));
            }
          };

          List<KV<Integer, String>> oo = new ArrayList<KV<Integer, String>>() {};

          c.element().getValue().forEach(oo::add);

          assertKvListEquals(o, oo);
        }

        // for case key = 1030
        if (c.element().getKey() == "1030") {
          List<KV<Integer, String>> o = new ArrayList<KV<Integer, String>>() {
            {
              add(KV.of(0, "1030"));
              add(KV.of(1, "1030"));
              add(KV.of(2, "1030"));
              add(KV.of(3, "1030"));
            }
          };

          List<KV<Integer, String>> oo = new ArrayList<KV<Integer, String>>() {};

          c.element().getValue().forEach(oo::add);

          assertKvListEquals(o, oo);
        }

        // for case key = 1999
        if (c.element().getKey() == "1999") {
          List<KV<Integer, String>> o = new ArrayList<KV<Integer, String>>() {
            {
              add(KV.of(0, "1999"));
              add(KV.of(3, "1999"));
              add(KV.of(9, "1999"));
              add(KV.of(9, "1999"));
            }
          };

          List<KV<Integer, String>> oo = new ArrayList<KV<Integer, String>>() {};

          c.element().getValue().forEach(oo::add);

          assertKvListEquals(o, oo);
        }
      }
    }
  }

  @Rule
  public final transient TestPipeline pipeline_testSerializableDoFnForEventSchemaAndSetCoderPCollections = TestPipeline.create();

  @Test // done
  @Category(ValidatesRunner.class)
  public void testSerializableDoFnForEventSchemaAndSetCoderPCollections() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p =
      pipeline_testSerializableDoFnForEventSchemaAndSetCoderPCollections;

    PCollection<KV<String, List<String>>> o = p
      .apply(
        App.Helper.randomCallerTag(),
        Create.<KV<String, List<String>>>of(
          KV.of("0", Arrays.asList(" ")),
          KV.of("1", Arrays.asList(" "))
        )
      )
      .setCoder(
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))
      );

    assertFalse(o.hasSchema());
    assertNotNull(o.getCoder());

    PCollection<Void> oo = o
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<KV<String, List<String>>, Void>())
      )
      .setCoder(VoidCoder.of());

    assertFalse(oo.hasSchema());
    assertNotNull(oo.getCoder());

    PCollection<KV<String, List<Event>>> ooo = oo
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<Void, KV<String, List<Event>>>())
      )
      .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(Event.CODER)));

    assertFalse(ooo.hasSchema());
    assertNotNull(ooo.getCoder());

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testGroupAndSort = TestPipeline.create();

  @Test // done
  @Category(ValidatesRunner.class)
  public void testGroupAndSort() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testGroupAndSort;

    PCollection<KV<String, List<KV<Integer, String>>>> eventData = App.<String>groupAndSort(
      p.apply(
        App.Helper.randomCallerTag(),
        Create.<KV<String, KV<Integer, String>>>of(
          (KV.of("1000", KV.of(1, "1000"))),
          (KV.of("1005", KV.of(1, "1005"))),
          (KV.of("1030", KV.of(1, "1030"))),
          (KV.of("1999", KV.of(3, "1999"))),
          (KV.of("1999", KV.of(0, "1999"))),
          (KV.of("1000", KV.of(2, "1000"))),
          (KV.of("1999", KV.of(9, "1999"))),
          (KV.of("1000", KV.of(4, "1000"))),
          (KV.of("1000", KV.of(3, "1000"))),
          (KV.of("1030", KV.of(0, "1030"))),
          (KV.of("1030", KV.of(2, "1030"))),
          (KV.of("1999", KV.of(9, "1999"))),
          (KV.of("1000", KV.of(5, "1000"))),
          (KV.of("1005", KV.of(3, "1005"))),
          (KV.of("1005", KV.of(4, "1005"))),
          (KV.of("1005", KV.of(2, "1005")))
        )
      )
    );

    eventData
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(
          new ParDoTransforms.Random<KV<String, List<KV<Integer, String>>>, Void>()
        )
      )
      .setCoder(VoidCoder.of());

    eventData.apply(
      App.Helper.randomCallerTag(),
      ParDo.of(new ParDoTransforms.AssertGroupAndSort())
    );

    PCollection<KV<String, KV<Integer, Event>>> pcol = p
      .apply(
        App.Helper.randomCallerTag(),
        Create.<KV<String, KV<Integer, Event>>>of(
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          ),
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          )
        )
      )
      .setCoder(
        KvCoder.of(
          StringUtf8Coder.of(),
          KvCoder.of(TextualIntegerCoder.of(), Event.CODER)
        )
      );

    PCollection<KV<String, List<KV<Integer, Event>>>> sorted = App.<Event>groupAndSort(
      pcol
    );

    sorted
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(
          new ParDoTransforms.Random<KV<String, List<KV<Integer, Event>>>, Void>()
        )
      )
      .setCoder(VoidCoder.of());

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testGroupAndSortAutoEncodable = TestPipeline.create();

  @Test // done
  @Category(ValidatesRunner.class)
  public void testGroupAndSortAutoEncodable() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testGroupAndSortAutoEncodable;

    PCollection<KV<String, KV<Integer, Event>>> pcol = p
      .apply(
        App.Helper.randomCallerTag(),
        Create.<KV<String, KV<Integer, Event>>>of(
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          ),
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          ),
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          ),
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          ),
          (
            KV.of(
              "NDA DC Equity",
              KV.of(
                42034206,
                Event
                  .builder()
                  .stockCode("NDA DC Equity")
                  .bidPrice(86.3d)
                  .askPrice(86.4d)
                  .tradePrice(86.35)
                  .bidVolume(24859)
                  .askVolume(9122)
                  .tradeVolume(897)
                  .updateType(3)
                  .dateTime(42034206)
                  .condition(" ")
                  .build()
              )
            )
          )
        )
      )
      .setCoder(
        KvCoder.of(
          StringUtf8Coder.of(),
          KvCoder.of(TextualIntegerCoder.of(), Event.CODER)
        )
      );

    PCollection<KV<String, List<KV<Integer, Event>>>> pcolList = App.<Event>groupAndSort(
      pcol
    );
    // .setCoder(
    // KvCoder.of(
    // StringUtf8Coder.of(),
    // ListCoder.of(KvCoder.of(TextualIntegerCoder.of(), Event.CODER))));

    pcolList
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(
          new ParDoTransforms.Random<KV<String, List<KV<Integer, Event>>>, Void>()
        )
      )
      .setCoder(VoidCoder.of());

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testCoderRegistrationOnCoderTypes = TestPipeline.create();

  // @depreciated
  @Test
  @Category(ValidatesRunner.class)
  public void testCoderRegistrationOnCoderTypes() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testCoderRegistrationOnCoderTypes;

    /** encoding approach I: coder registry approach */

    // NOTE: the stream is not auto encodable
    PCollection<KV<Integer, Event>> events = p.apply(
      App.Helper.randomCallerTag(),
      Create.<KV<Integer, Event>>of(
        KV.of(0, Event.builder().build()),
        KV.of(1, Event.builder().build())
      )
    );

    CoderRegistry
      .createDefault()
      .registerCoderProvider(
        CoderProviders.forCoder(
          new TypeDescriptor<KV<Integer, Event>>() {},
          KvCoder.of(TextualIntegerCoder.of(), Event.CODER)
        )
      );

    events
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<KV<Integer, Event>, Void>())
      )
      .setCoder(VoidCoder.of());

    p.run().waitUntilFinish();
  }

  @Test
  public void testBeamSchemaCoder() throws CoderException, IOException {
    log.info(App.Helper.randomCallerTag());

    Coder<Event> coder = Event.CODER;

    assertNotNull(coder);

    ByteArrayOutputStream bOutput = new ByteArrayOutputStream(1000);

    coder.encode(
      Event
        .builder()
        .stockCode("NDA DC Equity")
        .bidPrice(86.3d)
        .askPrice(86.4d)
        .tradePrice(86.35)
        .bidVolume(24859)
        .askVolume(9122)
        .tradeVolume(897)
        .updateType(3)
        .dateTime(42034206)
        .condition(" ")
        .build(),
      bOutput
    );

    byte b[] = bOutput.toByteArray();

    log.info(String.format("%d", b.length));

    ByteArrayInputStream bInput = new ByteArrayInputStream(b);

    Event event = coder.decode(bInput);

    log.info(event.toString());

    assertNotNull(event.getStockCode());
  }

  @Rule
  public final transient TestPipeline pipeline_testBeamAutoEncodableTypes = TestPipeline.create();

  // @Test // @depreciated
  @Category(ValidatesRunner.class)
  public void testBeamAutoEncodableTypes()
    throws CannotProvideCoderException, NoSuchSchemaException {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testBeamAutoEncodableTypes;

    /** encoding approach II: row schema approach */
    Schema schemaOfKvIntEvent = new Schema.Builder()
      .addStringField("stockCode")
      .addRowField("event", Event.getSchema().or(new Schema.Builder().build()))
      .build();

    // the stream is not auto encodable
    PCollection<KV<Integer, Event>> o = p
      .apply(
        App.Helper.randomCallerTag(),
        Create.<KV<Integer, Event>>of(
          KV.of(0, Event.builder().build()),
          KV.of(1, Event.builder().build())
        )
      )
      .setSchema(
        schemaOfKvIntEvent,
        new TypeDescriptor<KV<Integer, Event>>() {},
        new SerializableFunction<KV<Integer, Event>, Row>() {
          @Override
          public Row apply(KV<Integer, Event> kv) {
            return Row
              .withSchema(schemaOfKvIntEvent)
              .addValues(
                kv.getKey().toString(),
                Row
                  .withSchema(
                    Event.getSchema().or((new Schema.Builder()).build())
                  )
                  .build()
              )
              .build();
          }
        },
        new SerializableFunction<Row, KV<Integer, Event>>() {
          @Override
          public KV<Integer, Event> apply(Row row) {
            return KV.of(
              Integer.parseInt(row.getString("stockCode")),
              Event.builder().build()
            );
          }
        }
      );

    o.apply(
      App.Helper.randomCallerTag(),
      ParDo.of(new ParDoTransforms.Random<KV<Integer, Event>, Void>())
    );

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testAsJsons = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testAsJsons() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testAsJsons;

    PCollection<Event> events = p.apply(
      App.Helper.randomCallerTag(),
      Create.of(
        Event
          .builder()
          .stockCode("1000")
          .bidPrice(100d)
          .bidVolume(1000)
          .build(),
        Event
          .builder()
          .stockCode("1000")
          .bidPrice(100d)
          .bidVolume(1000)
          .build(),
        Event
          .builder()
          .stockCode("1000")
          .bidPrice(100d)
          .bidVolume(1000)
          .build(),
        Event.builder().stockCode("1000").bidPrice(100d).bidVolume(1000).build()
      )
    );

    events
      .apply(ToJson.<Event>of())
      .apply(
        App.Helper.randomCallerTag(),
        MapElements
          .into(TypeDescriptors.strings())
          .via((String encodedResult) -> {
            String s =
              "{\"id\":null,\"stockCode\":\"1000\",\"bidPrice\":100.0,\"askPrice\":null,\"tradePrice\":null,\"bidVolume\":1000,\"askVolume\":null,\"tradeVolume\":null,\"updateType\":null,\"dateTime\":null,\"condition\":null}";

            assertEquals(s.trim(), encodedResult.trim());

            return encodedResult;
          })
      );

    PCollection<ReportResult> reports = p.apply(
      App.Helper.randomCallerTag(),
      Create.of(
        ReportResult.builder().stockCode("1000").meanTradingTime(0.5d).build(),
        ReportResult.builder().stockCode("1000").meanTradingTime(0.5d).build(),
        ReportResult.builder().stockCode("1000").meanTradingTime(0.5d).build(),
        ReportResult.builder().stockCode("1000").meanTradingTime(0.5d).build(),
        ReportResult.builder().stockCode("1000").meanTradingTime(0.5d).build()
      )
    );

    reports
      .apply(AsJsons.<ReportResult>of(ReportResult.class))
      .apply(
        App.Helper.randomCallerTag(),
        MapElements
          .into(TypeDescriptors.strings())
          .via((String encodedResult) -> {
            String s =
              "{\"stockCode\":\"1000\",\"meanTradingTime\":0.5,\"meanTickChangesTime\":null,\"meanBidAskSpreads\":null,\"medianBidAskSpreads\":null,\"medianTradingTime\":null,\"medianTickChangesTime\":null,\"roundedTradedPriceBeZeroProb\":null,\"roundedTradedVolumeBeZeroProb\":null,\"longestTradingTime\":null,\"longestTickChangesTime\":null}";

            assertEquals(s.trim(), encodedResult.trim());

            return encodedResult;
          })
      );

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testKvCodeReportEncoder = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testKvCodeReportEncoder() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testKvCodeReportEncoder;

    p
      .getCoderRegistry()
      .registerCoderForClass(ReportResult.class, ReportResult.CODER);

    Coder<KV<String, ReportResult>> kvStockReportCoder = KvCoder.of(
      StringUtf8Coder.of(),
      ReportResult.CODER
    );

    // CoderRegistry
    // .createDefault()
    // .registerCoderProvider(
    // CoderProviders.forCoder(
    // new TypeDescriptor<KV<String, ReportResult>>() {},
    // kvStockReportCoder
    // )
    // );

    PCollection<KV<String, ReportResult>> o = pipeline_testKvCodeReportEncoder.apply(
      App.Helper.randomCallerTag(),
      Create.of(
        KV.of("1000", ReportResult.builder().build()),
        KV.of("1000", ReportResult.builder().build()),
        KV.of("1000", ReportResult.builder().build()),
        KV.of("1000", ReportResult.builder().build())
      )
    );

    // o.apply(
    // "[testKvCodeReportEncoder]: 1",
    // ParDo.of(new ParDoTransforms.Random<KV<String, ReportResult>, String>()))
    // .setCoder(StringUtf8Coder.of());

    PCollection<KV<String, ReportResult>> oo = PCollectionList
      .of(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of("1000", ReportResult.builder().build()),
            KV.of("1000", ReportResult.builder().build())
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of("1000", ReportResult.builder().build()),
            KV.of("1000", ReportResult.builder().build())
          )
        )
      )
      .apply(Flatten.<KV<String, ReportResult>>pCollections());

    // p.getCoderRegistry().registerCoderForClass(String.class,
    // StringUtf8Coder.of());

    oo
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<KV<String, ReportResult>, String>())
      )
      .setCoder(StringUtf8Coder.of());

    PAssert
      .that(oo.apply(App.Helper.randomCallerTag(), Count.globally()))
      .containsInAnyOrder(4l);

    p.run().waitUntilFinish();
  }

  public String someFunctionName(int i) {
    return App.Helper.getCallerFunctionName(i);
  }

  @Test
  public void testGetCallerFunctionName() {
    log.info(App.Helper.randomCallerTag());

    String s = someFunctionName(3);

    String ss = someFunctionName(2);

    assertEquals(s, "testGetCallerFunctionName");

    assertEquals(ss, "someFunctionName");
  }

  @Test
  public void testEncodeNullReport() throws CoderException, IOException {
    log.info(App.Helper.randomCallerTag());

    ReportResult report = ReportResult.builder().build();

    Coder<ReportResult> coder = ReportResult.CODER;

    ByteArrayOutputStream bOutput = new ByteArrayOutputStream(10000);

    coder.encode(ReportResult.builder().build(), bOutput);

    byte b[] = bOutput.toByteArray();

    log.info(String.format("byte array's length: %d", b.length));

    ByteArrayInputStream bInput = new ByteArrayInputStream(b);

    ReportResult decoded = coder.decode(bInput);

    log.info(decoded.toString());

    assertNotNull(decoded);
  }

  @Rule
  public final transient TestPipeline pipeline_testReportResultComputationSerializable = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testReportResultComputationSerializable() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testReportResultComputationSerializable;

    PCollection<KV<String, Double>> domainValues = p.apply(
      App.Helper.randomCallerTag(),
      Create.<KV<String, Double>>of(KV.of("1000", 0d), KV.of("1000", 0d))
    );
    // .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

    PCollection<KV<String, ReportResult>> o = App.computeReportResult(
      App.Helper.randomCallerTag(),
      ((ReportResult result, Double d) -> result),
      domainValues
    );
    // .setCoder(KvCoder.of(StringUtf8Coder.of(), ReportResult.CODER));

    o
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<KV<String, ReportResult>, String>())
      )
      .setCoder(StringUtf8Coder.of());

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testReportResultGroupByKey = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testReportResultGroupByKey() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testReportResultGroupByKey;

    PCollection<KV<String, ReportResult>> o = PCollectionList
      .of(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of(
              "100",
              ReportResult.builder().medianBidAskSpreads(0.1d).build()
            )
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of("100", ReportResult.builder().meanBidAskSpreads(0.4d).build())
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of("100", ReportResult.builder().meanTradingTime(0.4d).build())
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of(
              "100",
              ReportResult.builder().meanTickChangesTime(2.0d).build()
            )
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of("100", ReportResult.builder().meanBidAskSpreads(0.4d).build())
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of(
              "100",
              ReportResult.builder().medianBidAskSpreads(0.1d).build()
            )
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of(
              "100",
              ReportResult.builder().meanTickChangesTime(2.0d).build()
            )
          )
        )
      )
      .and(
        p.apply(
          App.Helper.randomCallerTag(),
          Create.<KV<String, ReportResult>>of(
            KV.of(
              "100",
              ReportResult.builder().roundedTradedPriceBeZeroProb(0.4d).build()
            )
          )
        )
      )
      .apply(Flatten.<KV<String, ReportResult>>pCollections());
    // .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

    // .setCoder(KvCoder.of(StringUtf8Coder.of(), ReportResult.CODER));
    o.apply(
      App.Helper.randomCallerTag(),
      App.Helper.<KV<String, ReportResult>>LogPTransform(kv -> {
        return "# reports: " + kv.getKey() + ": " + kv.getValue().toString();
      })
    );

    o
      .apply(
        App.Helper.randomCallerTag(),
        GroupByKey.<String, ReportResult>create()
      )
      .apply(
        App.Helper.<KV<String, Iterable<ReportResult>>>LogPTransform(kv -> {
          return "# grouped: " + kv.getKey() + ": " + kv.getValue().toString();
        })
      );

    o
      .apply(
        App.Helper.randomCallerTag(),
        ParDo.of(new ParDoTransforms.Random<KV<String, ReportResult>, String>())
      )
      .setCoder(StringUtf8Coder.of());

    p.run().waitUntilFinish();
  }

  @Rule
  public final transient TestPipeline pipeline_testFlatMapElements = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testAssertFlatMapElements() {
    TestPipeline p = pipeline_testFlatMapElements;

    p
      .apply(App.Helper.randomCallerTag(), Fixtures.eventListTransform)
      .apply(
        App.Helper.randomCallerTag(),
        MapElements
          .into(TypeDescriptors.nulls())
          .via(kvs -> {
            assert kvs.getValue().size() > 0;
            return null;
          })
      );

    p.run().waitUntilFinish();
  }

  // @Test
  public void testFixCodersEOFProblem() {
    log.info(App.Helper.randomCallerTag());

    // create event with EOF

    Coder<Event> coder = Event.CODER;

    // count time
    long start = System.currentTimeMillis();

    IntStream
      .range(0, 100)
      .reduce(
        0,
        (o_0, o_1) -> {
          try {
            Event event = Event
              .builder()
              .stockCode("NDA DC Equity")
              .bidPrice(86.3d)
              .askPrice(86.4d)
              .tradePrice(86.35)
              .bidVolume(24859)
              .askVolume(9122)
              .tradeVolume(897)
              .updateType(3)
              .dateTime(42034206)
              .condition(" ")
              .build();

            ByteArrayOutputStream bOutput = new ByteArrayOutputStream(10000);

            coder.encode(event, bOutput);

            byte b[] = bOutput.toByteArray();

            // log.info(String.format("byte array's length: %d", b.length));
            log.info("byte array's length: {}", b.length);

            ByteArrayInputStream bInput = new ByteArrayInputStream(b);

            Event decoded = coder.decode(bInput);

            assertNotNull(decoded);

            log.info(decoded.toString());
          } catch (Exception e) {}

          return 0;
        }
      );

    long end = System.currentTimeMillis();

    float sec = (end - start) / 1000F;

    log.info("execution time: {}, seconds", sec);
  }

  public String makeStockEventCacheKey(String stockCode) {
    return stockCode + "-Events";
  }

  /** test SystemCache roles in stock report processing */
  // @Test
  // public void testSystemCache() {
  // // sim. source of diff events
  // List<Event> events = new ArrayList<Event>();

  // SystemCache systemCache = new SystemCache();

  // // sim. event reading and grouping
  // events.forEach(e -> {

  // String cacheName = makeStockEventCacheKey(e.stockCode);

  // systemCache.saveCache(cacheName, e.stockCode, e);

  // });

  // // sim. get all events in one stock
  // systemCache.getCacheNames().forEach(cacheName -> {

  // List<Event> eventList = systemCache.getCacheAllValues(cacheName);

  // // sort list

  // // save sorted

  // // apply diff functionals

  // // save reports

  // })

  // // Cache<Integer, Event> cache = systemCache.createCache("events",
  // Integer.class,
  // Event.class, 100);

  // // sim. event

  // }

  @Rule
  public final transient TestPipeline pipeline_testStatefulComputations = TestPipeline.create();

  // @Test
  @Category(ValidatesRunner.class)
  public void testStatefulComputations() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testStatefulComputations;

    PCollection<Integer> ints = p
      .apply(App.Helper.randomCallerTag(), Fixtures.eventDataListTransform)
      .apply(
        ParDo.of(
          new DoFn<KV<String, List<Double>>, Integer>() {
            // A state cell holding a single Integer per key+window
            @StateId("index")
            private final StateSpec<ValueState<Integer>> indexSpec = StateSpecs.value(
              VarIntCoder.of()
            );

            @ProcessElement
            public void processElement(
              ProcessContext context,
              @StateId("index") ValueState<Integer> index
            ) {
              int current = MoreObjects.firstNonNull(index.read(), 0);

              context.output(current);

              index.write(current + 1);
            }
          }
        )
      );

    PAssert.that(ints).containsInAnyOrder(0, 1, 2);

    p.run().waitUntilFinish();
  }

  // public static JdbcDatabaseContainer pgsqlContainer = new
  // PostgreSQLContainer<>()
  // .withCreateContainerCmdModifier(cmd -> cmd.withName("test-containers-pgsql"))
  // //
  // .withMemory("1000")
  // .withLabel("test-containers", "pgsql")
  // .withDatabaseName("oem")
  // .withPassword("oem")
  // .withUsername("oem")
  // .withExposedPorts(5432)
  // .withReuse(true);

  public static class TryCatch {

    static void execute(Runnable func) {
      try {
        func.run();
      } catch (Exception e) {
        log.info("error");
        e.printStackTrace();
        log.info(e.toString());
      } finally {
        log.info("TryCatch::execute");
      }
    }
  }

  @Rule
  public final transient TestPipeline pipeline_testJdbcIOWrite = TestPipeline.create();

  // @Test
  @Category(ValidatesRunner.class)
  public void testJdbcIOWrite() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testJdbcIOWrite;

    // pgsqlContainer.start();

    // test write

    PCollection<EventListJdbcResult> results = p
      .apply(App.Helper.randomCallerTag(), Create.<String>of("ABB SS Equity"))
      .setCoder(StringUtf8Coder.of())
      .apply(
        App.Helper.randomCallerTag(),
        JdbcIO
          .<String>write()
          .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create(
              "com.impossibl.postgres.jdbc.PGDriver",
              "jdbc:pgsql://oem:oem@127.0.0.1:5432/oem"
            )
          )
          .withRetryConfiguration(
            JdbcIO.RetryConfiguration.create(
              5,
              Duration.standardSeconds(2),
              Duration.standardSeconds(1)
            )
          )
          .withRetryStrategy(exception -> true)
          // .withStatement("select count(*) from t_events where stock_code = 'ABB SS
          // Equity';")
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

            {
              var i = 0;

              while (rs.next()) {

                ++i;

                String eventString = "";

                for (var j = 1; j < 16; j++) {

                  if (j == 1) {
                    eventString = rs.getString(1);
                    continue;
                  }
                  // eventString += "," + String.format("%d:", j) + rs.getString(j);
                  eventString += "," + rs.getString(j);

                  // log.info(String.format("%d:", j));
                  // log.info(rs.getString(j));
                  // log.info(String.format("len:%d", rs.getString(j).length()));
                }

                eventString += ",";

                log.info("checking");
                log.info(eventString);

                events.add(Event.parseFromSourceLineString(eventString, 0));

                log.info(Integer.toString(rs.getFetchSize()));
              }
            }

            return EventListJdbcResult.builder().values(events).build();
            // return Event.builder().stockCode(resultSet.getString(1)).build();
          })
      )
      .setCoder(EventListJdbcResult.GetCoder(Event.CODER));

    // test read
    PCollection<KV<String, List<KV<Integer, Event>>>> sortedEvents = results
      .apply(
        App.Helper.randomCallerTag(),
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

    sortedEvents.apply(
      App.Helper.randomCallerTag(),
      App.Helper.<KV<String, List<KV<Integer, Event>>>>LogPTransform((KV<String, List<KV<Integer, Event>>> o) -> {
        log.info("checking-e");
        return o.toString();
      })
    );

    sortedEvents
    .apply(
      App.Helper.randomCallerTag(),
      MapElements.into(TypeDescriptors.strings()).via(e -> e.toString())
    )
    .apply(
      App.Helper.randomCallerTag(),
      TextIO.write().to("./outputs/testJdbcIOWrite.txt")
    );

    p.run().waitUntilFinish();
  }

  // @Container
  // final MongoDBContainer mongoDBContainer = new MongoDBContainer(
  // DockerImageName.parse("mongo:4.0.10")
  // )
  // .withCreateContainerCmdModifier(cmd -> cmd.withName("test-containers-mongo-"
  // +
  // RandomStringUtils.randomAlphanumeric(10))) // .withMemory("1000")
  // .withLabel("test-containers", "mongo")
  // .withExposedPorts(27017)
  // .wkithReuse(true)
  // // .dependsOn()
  // .withStartupCheckStrategy(
  // new IndefiniteWaitOneShotStartupCheckStrategy()
  // );

  @Rule
  public final transient TestPipeline pipeline_testMongodbIO = TestPipeline.create();

  // @Test
  @Category(ValidatesRunner.class)
  public void testMongodbIO() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testMongodbIO;

    // mongoDBContainer.start();
    // # testcontainer host wait until mongodb setup

    // mongoDBContainer.waitUntilContainerStarted();

    // if (true) while (true);

    // if (true) return;

    // test write
    p
      .apply(Fixtures.eventsCreationTransform)
      .apply(
        App.Helper.randomCallerTag(),
        MapElements
          .into(TypeDescriptor.of(org.bson.Document.class))
          .via((Event e) ->
            new org.bson.Document(
              Map.ofEntries(
                Map.entry("stockCode", e.getStockCode()),
                Map.entry("bidPrice", e.getBidPrice()),
                Map.entry("askPrice", e.getAskPrice()),
                Map.entry("tradePrice", e.getTradePrice()),
                Map.entry("bidVolume", e.getBidVolume()),
                Map.entry("askVolume", e.getAskVolume()),
                Map.entry("tradeVolume", e.getTradeVolume()),
                Map.entry("updateType", e.getUpdateType()),
                Map.entry("dateTime", e.getDateTime()),
                Map.entry("condition", e.getCondition())
              )
            )
          )
      )
      .apply(
        MongoDbIO
          .write()
          // .withUri("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))
          .withDatabase("oem")
          .withCollection("events")
        // .withNumSplits(5)
      );

    // test read
    p
      .apply(
        MongoDbIO
          .read()
          // .withUri("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))
          .withDatabase("oem")
          .withCollection("events")
      )
      .apply(
        App.Helper.randomCallerTag(),
        App.Helper.<String>LogPTransform(o -> o.toString())
      );

    p.run().waitUntilFinish();
    // mongoDBContainer.stop();

  }

  // test jsonb?

  // @Test
  public void testSpringJdbcTemplate() {
    String TAG = App.Helper.randomCallerTag();

    log.info(TAG);

    // String configFile = "./resources/db.properties";

    Properties config = new Properties();

    config.setProperty("jdbcUrl", "jdbc:pgsql://oem:oem@127.0.0.1:5432/oem");
    config.setProperty("dataSource.user", "oem");
    config.setProperty("dataSource.password", "oem");
    config.setProperty("dataSource.cachePrepStmts", "true");
    config.setProperty("dataSource.prepStmtCacheSize", "250");
    config.setProperty("dataSource.prepStmtSqlLimit", "2048");

    HikariConfig cfg = new HikariConfig(config);
    HikariDataSource dataSource = new HikariDataSource(cfg);

    // DataSource dataSource = new EmbeddedDatabaseBuilder()
    // .setType(EmbeddedDatabaseType.H2)
    // .build();

    // DataSource dataSource = new PGConnectionPoolDataSource();
    // dataSource.setUrl();

    JdbcTemplate jdbcTemplate = new JdbcTemplate();

    jdbcTemplate.setDataSource(dataSource);

    try {
      jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS T_TEMP_1 (id INT);");

      jdbcTemplate.update("INSERT INTO T_TEMP_ (id) VALUES (1234);");

      List<Integer> out = (List<Integer>) jdbcTemplate.queryForList(
        "SELECT * FROM T_TEMP_1;",
        Integer.class
        // (java.sql.ResultSet rs) -> rs.getInt(1)
      );

      List<Integer> assertResult = new ArrayList<Integer>() {
        {
          add(1234);
        }
      }; //

      log.info(out.toString());
    } catch (Exception e) {
      e.printStackTrace();
      log.info(e.toString());
    }
    // IntStream
    // .range(0, out.size())
    // .boxed()
    // .forEach(i -> assertTrue(out.get(i) == assertResult.get(i)));

    // Map<String, Object> out = (Map<String, Object>) jdbcTemplate.queryForObject(
    // "select 0;", Integer.class
    // );
    // out.forEach(i -> i)
    // assertTrue(out == 1);
    // log.info(out.toString());
  }

  @Rule
  public final transient TestPipeline pipeline_testJdbcIORead = TestPipeline.create();

  // @Test
  @Category(ValidatesRunner.class)
  public void testJdbcIORead() {
    log.info(App.Helper.randomCallerTag());

    TestPipeline p = pipeline_testJdbcIORead;

    // test read out distinct groupby with sorting

    PCollection<String> stockcodes = p.apply(
      App.Helper.randomCallerTag(),
      JdbcIO
        .<String>read()
        .withDataSourceConfiguration(
          JdbcIO.DataSourceConfiguration.create(
            "com.impossibl.postgres.jdbc.PGDriver",
            "jdbc:pgsql://oem:oem@127.0.0.1:5432/oem"
          )
        )
        .withQuery("select distinct(stock_code) from t_events;")
        .withRowMapper((java.sql.ResultSet resultSet) -> resultSet.getString(1))
        .withCoder(StringUtf8Coder.of())
    );

    stockcodes.apply(
      App.Helper.randomCallerTag(),
      App.Helper.<String>LogPTransform(o -> {
        log.info("testRead");
        log.info(o);
        return o;
      })
    );

    p.run().waitUntilFinish();
  }

  @Test
  public void testStrEqual() {
    log.info(App.Helper.randomCallerTag());
    String some0 = "420";
    String some1 = "420";
    assertTrue(some0 == some1);
    assertTrue(some0.equals(some1));
    // log.info("ok");
    assert true;
  }

  @Test
  public void testDiffInDays() {
    String some = "4.2029013E7, 4.2030294E7, 4.20322E7, 4.2046529E7, 4.2047015E7, 4.2047278E7, 4.2048665E7, 4.2049359E7, 4.2049819E7, 4.2050399E7, 4.2051374E7, 4.2052706E7, 4.2056162E7, 4.2132226E7, 4.2133118E7, 4.2133911E7, 4.2141838E7, 4.2143323E7, 4.2144396E7, 4.2144465E7, 4.2144605E7, 4.2145837E7, 4.2154738E7, 4.2157078E7, 4.2157598E7, 4.2158152E7, 4.2158238E7, 4.2158835E7, 4.2230912E7, 4.2231032E7, 4.2231055E7, 4.223445E7, 4.2234486E7, 4.2235251E7, 4.2239194E7, 4.2239916E7, 4.2242438E7, 4.2242951E7, 4.2248008E7, 4.224951E7, 4.2252518E7, 4.2253986E7, 4.2255767E7, 4.2255792E7, 4.2258122E7, 4.2258306E7, 4.2258328E7, 4.2258434E7, 4.2258665E7, 4.2332076E7, 4.2333636E7, 4.2333973E7, 4.2334109E7, 4.2335885E7, 4.2335957E7, 4.2336245E7, 4.2336557E7, 4.2337784E7, 4.2339232E7, 4.2339312E7, 4.2340017E7, 4.2342111E7, 4.2342232E7, 4.2342314E7, 4.2342319E7, 4.2342326E7, 4.2342338E7, 4.234234E7, 4.234234E7, 4.2342341E7, 4.2342344E7, 4.2342344E7, 4.2342345E7, 4.2342346E7, 4.234235E7, 4.234235E7, 4.2342366E7, 4.2342369E7, 4.2342369E7, 4.2342376E7, 4.2342376E7, 4.2342377E7, 4.234238E7, 4.2342386E7, 4.2342387E7, 4.2342405E7, 4.2342406E7, 4.2342433E7, 4.2342435E7, 4.2342442E7, 4.2342493E7, 4.2342493E7, 4.23425E7, 4.23425E7, 4.2342507E7, 4.2342515E7, 4.234252E7, 4.2342541E7, 4.2342542E7, 4.2342546E7, 4.2342561E7, 4.234259E7, 4.2342591E7, 4.2342618E7, 4.2342631E7, 4.2342644E7, 4.2342649E7, 4.2342671E7, 4.2342681E7, 4.2342684E7, 4.2342684E7, 4.2342696E7, 4.2342727E7, 4.2342742E7, 4.234278E7, 4.2342946E7, 4.2342948E7, 4.2342951E7, 4.2342953E7, 4.2342954E7, 4.2342966E7, 4.2342967E7, 4.2342971E7, 4.2342971E7, 4.2342975E7, 4.2342975E7, 4.2342978E7, 4.2342981E7, 4.2343018E7, 4.2343288E7, 4.2343371E7, 4.2343375E7, 4.2343493E7, 4.2343564E7, 4.2343565E7, 4.2343594E7, 4.2343601E7, 4.2343601E7, 4.2343639E7, 4.2343652E7, 4.2343664E7, 4.2343675E7, 4.2343689E7, 4.2343689E7, 4.2343831E7, 4.2343843E7, 4.234388E7, 4.2343921E7, 4.2343945E7, 4.2343991E7, 4.2344088E7, 4.2344089E7, 4.2344109E7, 4.2344198E7, 4.2344227E7, 4.2344228E7, 4.2344229E7, 4.2344269E7, 4.2344282E7, 4.2344315E7, 4.2344316E7, 4.2344335E7, 4.234448E7, 4.2344502E7, 4.2344521E7, 4.2344652E7, 4.2344666E7, 4.2345542E7, 4.2345806E7, 4.2345949E7, 4.2346216E7, 4.2346399E7, 4.2346547E7, 4.2346718E7, 4.2346755E7, 4.2347092E7, 4.2347147E7, 4.2347193E7, 4.2347223E7, 4.2347334E7, 4.234737E7, 4.2347379E7, 4.2347401E7, 4.2347406E7, 4.2347406E7, 4.2347437E7, 4.2347496E7, 4.2347523E7, 4.2347557E7, 4.234756E7, 4.2347579E7, 4.2347583E7, 4.2347599E7, 4.2347617E7, 4.2347698E7, 4.2347729E7, 4.2347729E7, 4.2347731E7, 4.2347757E7, 4.2347795E7, 4.2347814E7, 4.2347974E7, 4.2348066E7, 4.2348067E7, 4.2348069E7, 4.2348249E7, 4.2348546E7, 4.23486E7, 4.2348707E7, 4.2348742E7, 4.2348863E7, 4.2348869E7, 4.2348923E7, 4.234896E7, 4.2349243E7, 4.2349424E7, 4.234969E7, 4.23497E7, 4.2349702E7, 4.234972E7, 4.2349731E7, 4.2349741E7, 4.2349927E7, 4.2349937E7, 4.2349939E7, 4.2349978E7, 4.2350013E7, 4.2350014E7, 4.2350121E7, 4.2350245E7, 4.235032E7, 4.2350422E7, 4.2350504E7, 4.2350681E7, 4.2350705E7, 4.2350719E7, 4.2351111E7, 4.235148E7, 4.2351702E7, 4.2351771E7, 4.235236E7, 4.2352439E7, 4.2352476E7, 4.2352557E7, 4.2352573E7, 4.2352797E7, 4.2353138E7, 4.2353249E7, 4.2354003E7, 4.2354081E7, 4.2354451E7, 4.2354647E7, 4.2354745E7, 4.2354934E7, 4.2354985E7, 4.2355485E7, 4.2356052E7, 4.2356349E7, 4.2356408E7, 4.2356457E7, 4.2356519E7, 4.2356566E7, 4.2356587E7, 4.2356603E7, 4.2356631E7, 4.2356834E7, 4.2356889E7, 4.2357079E7, 4.2357114E7, 4.2357123E7, 4.2357141E7, 4.2357143E7, 4.2357147E7, 4.235725E7, 4.2357336E7, 4.2357389E7, 4.2357545E7, 4.2358111E7, 4.23582E7, 4.2358272E7, 4.2358418E7, 4.2358544E7, 4.2358584E7, 4.2358594E7, 4.2358833E7, 4.2358905E7, 4.2358944E7, 4.2358957E7, 4.2359027E7, 4.235908E7";
    List<String> someList = Arrays.asList(some.split(", "));
    List<Double> someDoubleList = someList.stream().map(s -> Double.parseDouble(s)).collect(Collectors.toList());
    // log.info(someDoubleList.toString());
    List<Double> someDiffList = App.diffInDays(someDoubleList);
    // log.info(someDiffList.toString());
    assert true;
  }
}
