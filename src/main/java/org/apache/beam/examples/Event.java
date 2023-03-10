package org.apache.beam.examples;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
// import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.jdbc.JdbcWriteResult;

// @CommonsLog
// @Slf4j
@Accessors(chain = true)
@Data
@lombok.Builder
@DefaultSchema(JavaFieldSchema.class)
public class Event extends JdbcWriteResult {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Event.class);

    private static final boolean DEBUG = false;

    public static Coder<Event> CODER = null;

    public static final String NULL_STRING_REPR = "+";

    static {
        // initCoder();
    }

    public static void initCoder() {
        if (CODER != null)
            return;

        log.info("Event: init Coder");

        SchemaRegistry s = SchemaRegistry.createDefault();

        CoderRegistry c = CoderRegistry.createDefault();

        Schema eventSchema = Event.getSchema().get();

        try {
            Coder<Event> eventCoder = new Coder<Event>() {
                final ObjectMapper om = new ObjectMapper();

                public void verifyDeterministic() {
                }

                // @depreciated
                private String readString(java.io.InputStream dis) throws IOException {
                    int len = VarInt.decodeInt(dis);

                    if (len < 0) {
                        throw new CoderException("Invalid encoded string length: " + len);
                    }

                    byte[] bytes = new byte[len];

                    ByteStreams.readFully(dis, bytes);

                    return new String(bytes, StandardCharsets.UTF_8);
                }

                @Override
                public Event decode(java.io.InputStream inputStream) {
                    try {
                        /** approach I */
                        // byte[] bytes = inputStream.readAllBytes();
                        // String utf8Str = new String(bytes, StandardCharsets.UTF_8).trim();

                        /** approach II */
                        // StringUtf8Coder coder = (StringUtf8Coder) getCoderArguments().get(0);

                        StringUtf8Coder coder = StringUtf8Coder.of();

                        if (DEBUG) {
                            log.info("debug: decode: start");
                        }

                        // String jsonStr = coder.decode(inputStream, new Coder.Context(true));
                        String jsonStr = coder.decode(inputStream);
                        // String jsonStr = utf8Str;

                        if (DEBUG) {
                            log.info("debug: decode: finished");
                            log.info(jsonStr);
                        }

                        Event e = om.readValue(jsonStr, Event.class);

                        if (DEBUG) {
                            log.info("debug: om: ok");
                            log.info(e.toString());
                        }

                        return e;
                    } catch (Exception exp) {
                        log.info("debug: decode: failed");
                        exp.printStackTrace();
                    }

                    return Event.builder().build();
                }

                @Override
                public void encode(Event e, java.io.OutputStream outStream) {
                    try {
                        String str = om.writeValueAsString(e).trim();

                        if (DEBUG) {
                            log.info("debug: encode: tojson: ok");
                            log.info(str);
                        }

                        // StringUtf8Coder coder = (StringUtf8Coder) getCoderArguments().get(0);
                        StringUtf8Coder coder = StringUtf8Coder.of();

                        // check if str EOF?

                        // coder.encode(str + "\n", outStream, new Coder.Context(true));
                        // coder.encode(str + "\n", outStream, new Coder.Context(true));
                        coder.encode(str + "\n", outStream);
                        // coder.encode(str", outStream);

                        if (DEBUG) {
                            log.info("debug: encode: toBytes: ok");
                        }
                    } catch (Exception exp) {
                        log.info("debug: encode: failed");
                        exp.printStackTrace();
                    }
                }

                @Override
                public List<? extends Coder<?>> getCoderArguments() {
                    return new ArrayList() {
                        {
                            add(StringUtf8Coder.of());
                        }
                    };
                }
            };

            c.registerCoderForClass(Event.class, eventCoder);

            // CODER = c.getCoder(Event.class);

            CODER = c.getCoder(Event.class);
        } catch (CannotProvideCoderException e) {
            throw new ExceptionInInitializerError(e);
        }

        // @depreciated
        if (false) {
            // make field name -> fields map
            final Map<String, Field> eventFieldValues = Arrays.asList(Event.class.getDeclaredFields()).stream()
                    .parallel().collect(Collectors.toMap(field -> field.getName(), field -> field));

            c.registerCoderForClass(Event.class, SchemaCoder.<Event> of(eventSchema, new TypeDescriptor<Event>() {
            }, new SerializableFunction<Event, Row>() {
                @Override
                public Row apply(Event e) {
                    Row.Builder rowBuilder = Row.withSchema(eventSchema);

                    List<Schema.Field> schemaFields = eventSchema.getFields();

                    for (Schema.Field schemaField : schemaFields) {
                        Field fieldValueGetter = eventFieldValues.get(schemaField.getName());

                        fieldValueGetter.setAccessible(true);

                        try {
                            Object value = fieldValueGetter.get(e);
                            // add row value
                            rowBuilder.addValue(value);
                        } catch (java.lang.IllegalAccessException exp) {
                            exp.printStackTrace();
                            rowBuilder.addValue(null);
                        }
                    }

                    // build row
                    Row row = rowBuilder.build();

                    // log.info(row.toString());

                    return row;
                }
            }, new SerializableFunction<Row, Event>() {
                @Override
                public Event apply(Row row) {
                    // log.info(row.toString());

                    Event e = Event.builder().build();

                    List<Schema.Field> schemaFields = eventSchema.getFields();

                    for (Schema.Field schemaField : schemaFields) {
                        Field fieldValueSetter = eventFieldValues.get(schemaField.getName());

                        fieldValueSetter.setAccessible(true);

                        try {
                            fieldValueSetter.set(e, row.getValue(fieldValueSetter.getName()));
                        } catch (java.lang.IllegalAccessException exp) {
                            exp.printStackTrace();
                        }
                    }

                    return e;
                }
            }));
        }
        // catch (CannotProvideCoderException e) {
        // throw new ExceptionInInitializerError(e);
        // }
    }

    public static Optional<Schema> getSchema() {
        Schema out = null;
        try {
            SchemaRegistry s = SchemaRegistry.createDefault();
            out = s.getSchema(Event.class);
            return Optional.fromNullable(out);
        } catch (NoSuchSchemaException e) {
            e.printStackTrace();
            return Optional.fromNullable(out);
        } finally {
            return Optional.fromNullable(out);
        }
    }

    @Nullable
    @JsonProperty
    public Integer id; // 1

    /**
     * 1 = Bloomberg Code/Stock identifier 3 = Bid Price 4 = Ask Price 5 = Trade Price 6 = Bid Volume 7 = Ask Volume 8 =
     * Trade Volume 9 = Update type => 1=Trade; 2= Change to Bid (Px or Vol); 3=Change to Ask (Px or Vol) 11 = Date 12 =
     * Time in seconds past midnight 15 = Condition codes
     */
    @Nullable
    @JsonProperty
    public String stockCode; // 1

    @Nullable
    @JsonProperty
    public Double bidPrice; // 3

    @Nullable
    @JsonProperty
    public Double askPrice; // 4

    @Nullable
    @JsonProperty
    public Double tradePrice; // 5

    @Nullable
    @JsonProperty
    public Integer bidVolume; // 6

    @Nullable
    @JsonProperty
    public Integer askVolume; // 7

    @Nullable
    @JsonProperty
    public Integer tradeVolume; // 8

    @Nullable
    @JsonProperty
    public Integer updateType; // 9

    @Nullable
    @JsonProperty
    public Integer dateTime; // 11, 12

    @Nullable
    @JsonProperty
    public String condition; // 15

    @SuppressWarnings("unused")
    public Event() {
        // log.info(this.toString());
    }

    // for @Builder and auto. encoder
    @SchemaCreate
    public Event(
            Integer id,
            String stockCode, // 1
            Double bidPrice, // 3
            Double askPrice, // 4
            Double tradePrice, // 5
            Integer bidVolume, // 6
            Integer askVolume, // 7
            Integer tradeVolume, // 8
            Integer updateType, // 9
            Integer dateTime, // 11, 12
            String condition // 15
    ) {
        this.id = id;
        this.stockCode = stockCode; // 1
        this.bidPrice = bidPrice; // 3
        this.askPrice = askPrice; // 4
        this.tradePrice = tradePrice; // 5
        this.bidVolume = bidVolume; // 6
        this.askVolume = askVolume; // 7
        this.tradeVolume = tradeVolume; // 8
        this.updateType = updateType; // 9
        this.dateTime = dateTime; // 11, 12
        this.condition = condition; // 15
        // log.info(this.toString());
    }

    public static Event parseFromSourceLineString(String line, Integer id) {
        String[] entries = line.split(",", -1);

        Event event = Event.builder().id(id).stockCode(entries[1 - 1]).bidPrice(Double.parseDouble(entries[3 - 1]))
                .askPrice(Double.parseDouble(entries[4 - 1])).tradePrice(Double.parseDouble(entries[5 - 1]))
                .bidVolume(Integer.parseInt(entries[6 - 1])).askVolume(Integer.parseInt(entries[7 - 1]))
                .tradeVolume(Integer.parseInt(entries[8 - 1])).updateType(Integer.parseInt(entries[9 - 1]))
                .dateTime(Integer.parseInt(
                        Event.Helpers.formatDoubleDateStringToIntegerString(entries[11 - 1], entries[12 - 1])))
                .condition(Event.Helpers.convertToNonNullString(entries[15 - 1])).build();

        return event;
    }

    public static class Helpers {

        public static String formatDoubleDateStringToIntegerString(@NonNull String yearString,
                @NonNull String dateString) {

            yearString = yearString.split("\\.", 3)[0];

            yearString = yearString.trim();

            int n = yearString.length();

            yearString = yearString.substring(n - Math.min(n, 3));

            dateString = dateString.split("\\.", 3)[0].trim();

            return yearString + dateString;
        }

        public static String convertToNonNullString(@NonNull String o) {
            if (o.equals("") || o.equals(" ")) {
                // throw new Error("Empty String");
                return NULL_STRING_REPR; // ".";
            }
            return o;
        }

        // @depreciated
        public static Event fromStringFormat(String value) {

            String[] values = value.split("|");

            return Event.builder().stockCode(values[0]).bidPrice(Double.parseDouble(values[1]))
                    .askPrice(Double.parseDouble(values[2])).tradePrice(Double.parseDouble(values[3]))
                    .bidVolume(Integer.parseInt(values[4])).bidVolume(Integer.parseInt(values[5]))
                    .tradeVolume(Integer.parseInt(values[6])).updateType(Integer.parseInt(values[7]))
                    .dateTime(Integer.parseInt(values[8])).condition(values[9]).build();

        }
    }

    public static enum Key {
        StockCode, BidPrice, AskPrice, TradePrice, TradeVolume, BidVolume, AskVolume, UpdateType, Date, Time, Condition,
    }

    public static class Utils {

        public static byte[] getBytes(InputStream is) throws IOException {
            int len;
            int size = 1024;
            byte[] buf;

            if (is instanceof ByteArrayInputStream) {
                size = is.available();
                buf = new byte[size];
                len = is.read(buf, 0, size);
            } else {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                buf = new byte[size];
                while ((len = is.read(buf, 0, size)) != -1)
                    bos.write(buf, 0, len);
                buf = bos.toByteArray();
            }
            return buf;
        }
    }
}
