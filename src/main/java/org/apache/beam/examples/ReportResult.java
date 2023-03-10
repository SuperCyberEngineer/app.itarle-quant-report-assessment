package org.apache.beam.examples;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import java.lang.reflect.Field;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.log4j.*;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// import lombok.extern.slf4j.Slf4j;

// import lombok.extern.slf4j.*;
// import lombok.extern.apachecommons.CommonsLog;

// @Log
// @Log4j2
// @Slf4j
@DefaultSchema(JavaFieldSchema.class)
@Accessors(chain = true)
@Data
@lombok.Builder
class ReportResult {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ReportResult.class);

    public static Coder<ReportResult> CODER = null;

    private static final boolean DEBUG = false;

    static {
        // initCoder();
    };

    public static void initCoder() {

        if (CODER != null)
            return;

        log.info("ReportResult: init Coder");

        try {

            SchemaRegistry s = SchemaRegistry.createDefault();

            CoderRegistry c = CoderRegistry.createDefault();

            Coder<ReportResult> reportResultCoder = new Coder<ReportResult>() {

                final ObjectMapper om = new ObjectMapper();

                public void verifyDeterministic() {
                }

                @Override
                public ReportResult decode(java.io.InputStream inputStream) {
                    try {

                        if (DEBUG) {
                            log.info("decode: start");
                        }

                        /** approach I */
                        // // byte[] bytes = inputStream.readAllBytes();
                        // String utf8Str = new String(bytes, StandardCharsets.UTF_8).trim();

                        /** approach II */
                        StringUtf8Coder coder = StringUtf8Coder.of();
                        // StringUtf8Coder coder = (StringUtf8Coder) getCoderArguments()
                        // .get(0);

                        String jsonStr = coder.decode(inputStream);
                        // , new Coder.Context(true));

                        if (DEBUG) {
                            log.info("decode: end");
                            log.info(jsonStr);
                        }

                        return om.readValue(jsonStr, ReportResult.class);

                        // return om.readValue(utf8Str, ReportResult.class);

                    } catch (Exception exp) {
                        log.info("decode: failed !!`");
                        log.info(exp.toString());
                        exp.printStackTrace();
                    }

                    return ReportResult.builder().build();
                }

                @Override
                public void encode(ReportResult report, java.io.OutputStream outStream) {
                    try {
                        if (DEBUG) {
                            log.info("report: encode: start");
                            log.info(report.toString());
                        }

                        String str = om.writeValueAsString(report);

                        assert str != "";

                        // str += "\n";

                        StringUtf8Coder coder = StringUtf8Coder.of();
                        // StringUtf8Coder coder = (StringUtf8Coder) getCoderArguments()
                        // .get(0);

                        coder.encode(str + "\n", outStream);
                        // , new Coder.Context(true));

                        if (DEBUG) {
                            log.info("report: encode: finish");
                        }

                    } catch (Exception exp) {
                        log.info("encode: failed !");
                        log.info(exp.toString());
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

            c.registerCoderForClass(ReportResult.class, reportResultCoder);

            CODER = c.getCoder(ReportResult.class);

        } catch (CannotProvideCoderException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused")
    public ReportResult() {
    }

    // for @Builder and auto. encoder
    // for BEAM schema objects
    @SchemaCreate
    public ReportResult(
        String stockCode,
        Double meanTradingTime,
        Double meanTickChangesTime,
        Double meanBidAskSpreads,
        Double medianBidAskSpreads,
        Double medianTradingTime,
        Double medianTickChangesTime,
        Double roundedTradedPriceBeZeroProb,
        Double roundedTradedVolumeBeZeroProb,
        Double longestTradingTime,
        Double longestTickChangesTime
    ) {
        this.stockCode = stockCode;
        this.meanTradingTime = meanTradingTime;
        this.meanTickChangesTime = meanTickChangesTime;
        this.meanBidAskSpreads = meanBidAskSpreads;
        this.medianTradingTime = medianTradingTime;
        this.medianTickChangesTime = medianTickChangesTime;
        this.medianBidAskSpreads = medianBidAskSpreads;
        this.longestTickChangesTime = longestTickChangesTime;
        this.longestTradingTime = longestTradingTime;
        this.roundedTradedPriceBeZeroProb = roundedTradedPriceBeZeroProb;
        this.roundedTradedVolumeBeZeroProb = roundedTradedVolumeBeZeroProb;
    }

    @Nullable
    @JsonProperty
    public String stockCode;

    @Nullable
    @JsonProperty
    public Double meanTradingTime;

    @Nullable
    @JsonProperty
    public Double meanTickChangesTime;

    @Nullable
    @JsonProperty
    public Double meanBidAskSpreads;

    @Nullable
    @JsonProperty
    public Double medianBidAskSpreads;

    @Nullable
    @JsonProperty
    public Double medianTradingTime;

    @Nullable
    @JsonProperty
    public Double medianTickChangesTime;

    @Nullable
    @JsonProperty
    public Double roundedTradedPriceBeZeroProb;

    @Nullable
    @JsonProperty
    public Double roundedTradedVolumeBeZeroProb;

    @Nullable
    @JsonProperty
    public Double longestTradingTime;

    @Nullable
    @JsonProperty
    public Double longestTickChangesTime;

    private static List<Field> _internalFields;

    // @depreciated
    public static List<Field> InternalFields() {
        if (_internalFields == null) {
            _internalFields = Arrays.asList(ReportResult.class.getDeclaredFields()).stream().parallel()
                    .filter(f -> !f.getName().equals("_internalFields") || !f.getName().equals("log"))
                    .collect(Collectors.toList());

            log.info(_internalFields.toString());

            return _internalFields;
        }

        return _internalFields;
    }

    public ReportResult addResult(@NonNull ReportResult result) {
        try {

            // filled required fields using java reflections
            for (Field field : ReportResult.class.getFields()) {

                Object value = field.get(result);

                if (value != null) {
                    field.set(this, value);
                }
            }

            return this;
        } catch (java.lang.IllegalAccessException e) {
            e.printStackTrace();
        }
        return this;
    }

    public static Optional<Schema> getSchema() {
        Schema out = null;
        try {
            SchemaRegistry s = SchemaRegistry.createDefault();
            out = s.getSchema(ReportResult.class);
        } catch (NoSuchSchemaException e) {
            e.printStackTrace();
        } finally {
            return Optional.fromNullable(out);
        }
    }

    @Override
    public boolean equals(Object obj) {
        try {
            for (Field field : ReportResult.class.getFields()) {

                field.setAccessible(true);

                Object value = field.get(obj);

                if (value == null && field.get(this) != null)
                    return false;

                if (value != null && !value.equals(field.get(this)))
                    return false;
            }

            return true;

        } catch (java.lang.IllegalAccessException e) {
            e.printStackTrace();
        }

        return false;
    }
}
