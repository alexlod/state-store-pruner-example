package io.confluent.ps.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class MyKafkaStreams {
  private static final Logger log = LoggerFactory.getLogger(MyKafkaStreams.class);

  public static final String OUTPUT_TOPIC = "test-out";

  public static final String STATE_STORE_NAME = "state-store";

  private static Properties createConfig() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-pruner-example");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/state-store-pruner-state");
    return props;
  }

  public static void main(final String[] args) {
    log.info("Starting...");

    StreamsBuilder builder = new StreamsBuilder();

    // `retainDuplicates` must be false so when the same key and timestamp are used to update a value,
    //  only one value will be stored.
    //
    // `retentionPeriod` must be slightly larger than `windowSize`. Because this example is only
    // using a WindowStore to expire old keys, the `retentionPeriod` and `windowSize` can be roughly
    // the same size.
    StoreBuilder<WindowStore<String, String>> storeBuilder =
            Stores.windowStoreBuilder(Stores.persistentWindowStore(STATE_STORE_NAME, Duration.ofDays(4), Duration.ofDays(3), false),
                    Serdes.String(), Serdes.String());
    builder.addStateStore(storeBuilder);

    KStream<String, String> messages = builder.stream(MyProducer.INPUT_TOPIC);
    messages.transform(MyTransformer::new, STATE_STORE_NAME);
    messages.to(OUTPUT_TOPIC);

    KafkaStreams app = new KafkaStreams(builder.build(), createConfig());

    app.cleanUp();
    app.start();

    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
  }

  private static class MyTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private WindowStore<String, String> stateStore;

    @Override
    public void init(ProcessorContext context) {
      this.stateStore = (WindowStore<String, String>) context.getStateStore(STATE_STORE_NAME);
    }

    /*
     * This shows how to use a WindowStore like a KeyValueStore. The reason to use a WindowStore in this way is
     * to benefit from the WindowStore's TTL functionality, where old keys are expired automatically.
     *
     * The overall flow to use a WindowStore in this way is as follows:
     *  - to get, simply fetch the first value for a given key
     *  - to put a new key/value pair, use put and make the `windowStartTimestamp` the current wall clock time.
     *  - to put an existing key/value pair, first fetch the existing windowStartTime, then put a new value but maintain
     *    the existing windowStartTimestamp to ensure the key is expired at the correct time and to ensure the
     *    existing value is overwritten.
     */
    @Override
    public KeyValue<String, String> transform(String key, String value) {
      log.info("TRANSFORM key: '" + key + "', value: '" + value + "'");

      // get the value and the window timestamp from the state store.
      KeyValue<Windowed<String>, String> windowedKeyValue = getWindowedKeyValue(this.stateStore, key);
      String storeValue = null;
      Long storeTimestamp = null;

      if (windowedKeyValue != null) {
        storeValue = windowedKeyValue.value;
        storeTimestamp = windowedKeyValue.key.window().start();
      }

      log.info("FETCH key: '" + key + "', value: '" + storeValue + "', timestamp: " + storeTimestamp);
      dumpKey(this.stateStore, key);

      // because the key doesn't exist in the store yet, set the timestamp
      // to be the current wall clock time.
      if (storeTimestamp == null) {
        storeTimestamp = System.currentTimeMillis();
      }

      // this shows how to put a value into the state store.
      // the `windowStartTimestamp` should be equal to the previous value's window time so
      // the key will be expired when intended. If the current wall clock time is used instead,
      // the key will survive longer than intended.
      this.stateStore.put(key, value, storeTimestamp);
      log.info("PUT key: '" + key + "', value: " + value + "', timestamp: " + storeTimestamp);
      dumpKey(this.stateStore, key);

      // do nothing to the actual message, because in this example I'm only showing how to interact with the state store.
      return KeyValue.pair(key, value);
    }

    @Override
    public void close() {

    }

    private static KeyValue<Windowed<String>, String> getWindowedKeyValue(WindowStore<String, String> store, String key) {
      // need to this particular `fetch` API to get the window timestamp.
      KeyValueIterator<Windowed<String>, String> iterator = store.fetch(key, key, 0, System.currentTimeMillis());

      KeyValue<Windowed<String>, String> toReturn = null;
      if (iterator.hasNext()) {
        toReturn = iterator.next();
      }

      iterator.close();
      return toReturn;
    }

    private static void dumpKey(WindowStore<String, String> store, String key) {
      KeyValueIterator<Windowed<String>, String> iterator = store.fetch(key, key, 0, System.currentTimeMillis());

      log.info("Dumping values for key '" + key + "'");

      int count = 0;
      while (iterator.hasNext()) {
        KeyValue<Windowed<String>, String> windowedKey = iterator.next();
        log.info("\tvalue: " + windowedKey.value + ", start: " + windowedKey.key.window().start() +
                 ", end: " + windowedKey.key.window().end());
        count++;
      }

      if (count == 0) {
        log.info("\t<none>");
      }

      iterator.close();
    }
  }
}
