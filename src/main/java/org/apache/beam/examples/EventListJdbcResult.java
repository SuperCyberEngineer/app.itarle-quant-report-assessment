package org.apache.beam.examples;

import org.apache.beam.sdk.io.jdbc.JdbcWriteResult;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import java.util.List;
import java.util.ArrayList;

@Accessors(chain = true)
@Data
@lombok.Builder
public class EventListJdbcResult extends JdbcWriteResult {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(EventListJdbcResult.class);

    private List<Event> values;

    private static Coder<EventListJdbcResult> CODER;

    public static Coder<EventListJdbcResult> GetCoder(Coder<Event> eventCoder){

        if (CODER != null) return CODER;

        Coder<List<Event>> listEventCoder = ListCoder.of(eventCoder);

        CODER = new Coder<EventListJdbcResult>() {

                public void verifyDeterministic() {
                }

                @Override
                public EventListJdbcResult decode(java.io.InputStream inputStream) {

                    try {

                        List<Event> decoded = listEventCoder.decode(inputStream, new Coder.Context(true));

                        return EventListJdbcResult.builder().values(decoded).build();

                    } catch (Exception exp) {
                        log.info("decode: failed");
                        exp.printStackTrace();
                    }

                    return EventListJdbcResult.builder().build();
                }

                @Override
                public void encode(EventListJdbcResult e, java.io.OutputStream outStream) {
                    try {

                        if (e.getValues() == null) return; 

                        listEventCoder.encode(e.getValues(), outStream, new Coder.Context(true));

                    } catch (Exception exp) {
                        log.info("encoded: failed");
                        exp.printStackTrace();
                    }
                }

                @Override
                public List<? extends Coder<?>> getCoderArguments() {
                    return new ArrayList() {
                    };
                }
            };

        return CODER;
    }
}