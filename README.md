This Kafka Streams example shows how to use a state store where old keys
are eventually expired (or pruned).

The way this is done is by using a `WindowStore` instead of a
`KeyValueStore`.

As you'll see in the example, this requires using the `WindowStore`
functionality in a bit of a specialized way because `WindowStore` is
designed to store different values for a given key based on what window
the value was updated in.

To run this example, first run `MyProducer` then run `MyKafkaStreams`.

If you'd like to perform a second run with a fresh start, use a
different `MyProducer.INPUT_TOPIC`, use a different
`APPLICATION_ID_CONFIG` in `MyKafkaStreams`, and delete
`/tmp/state-store-pruner-state`.
