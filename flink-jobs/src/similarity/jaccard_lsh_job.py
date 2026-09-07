from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Duration
import redis
import pickle
from datasketch import MinHash, MinHashLSH

# Continuous Stateful Jaccard Similarity using MinHash LSH
class JaccardSimilarity(KeyedProcessFunction):

    def open(self, context: RuntimeContext):
        self.redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        try:
            print("Connected to Redis!")
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise e
            
        # MapState to store user_id -> pickled MinHash signature
        sig_descriptor = MapStateDescriptor("user_signatures", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        self.signature_state = context.get_map_state(sig_descriptor)
        
        self.lsh_initialized = False
        self.lsh = MinHashLSH(threshold=0.5, num_perm=128)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out):
        # 1. On the very first event (or after restart), rebuild the in-memory LSH from Flink State
        if not self.lsh_initialized:
            print("DEBUG - Rebuilding LSH index from Flink MapState...")
            count = 0
            for uid, pickled_m in self.signature_state.items():
                m = pickle.loads(pickled_m)
                self.lsh.insert(uid, m)
                count += 1
            print(f"DEBUG - Restored {count} signatures into LSH.")
            self.lsh_initialized = True

        userid = value[0]
        interests = set(value[1]) if value[1] else set()
        
        if not interests:
            return

        # 2. Compute new MinHash signature for the user
        m = MinHash(num_perm=128)
        for interest in interests:
            m.update(interest.encode('utf-8'))

        # 3. Save to fault-tolerant Flink State
        self.signature_state.put(userid, pickle.dumps(m))
        
        # 4. Update the in-memory LSH
        if userid in self.lsh.keys:
            self.lsh.remove(userid)
        self.lsh.insert(userid, m)

        # 5. Query for matches continuously
        candidates = self.lsh.query(m)

        for candidate_id in candidates:
            if candidate_id != userid:
                # 6. Check Redis to prevent re-pairing already paired users
                u1group = self.redis_client.lrange(userid, 0, -1)
                u2group = self.redis_client.lrange(candidate_id, 0, -1)
                if userid in u2group and candidate_id in u1group:
                    continue

                print(f"DEBUG - LSH Match Found! {userid} <-> {candidate_id}")
                # Emit the matched pair
                yield Row(grouped=[userid, candidate_id], content=list(interests))


# TimestampAssigner: Uses Kafka record_timestamp for event time
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp) -> int:
        timestamp = int(record_timestamp) if record_timestamp else 0
        return timestamp


# Setup execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # Required for global state routing to single KeyedProcessFunction

env.get_config().set_auto_watermark_interval(200)

connector_jars = ";".join([
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar",
    "file:///opt/flink/lib/kafka-clients-3.6.0.jar"
])
env.add_jars(connector_jars)

# Input/Output row types
row_type = Types.ROW_NAMED(
    ["userid", "interests"],
    [Types.STRING(), Types.BASIC_ARRAY(Types.STRING())]
)

row_type_output = Types.ROW_NAMED(
    ["grouped","content"],
    [Types.BASIC_ARRAY(Types.STRING()), Types.BASIC_ARRAY(Types.STRING())]
)

# Configure Kafka consumer
deserialization_schema = JsonRowDeserializationSchema.builder().type_info(row_type).build()

kafka_consumer = FlinkKafkaConsumer(
    topics=["user-events"],
    deserialization_schema=deserialization_schema,
    properties={
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'pyflink-consumer-continuous-lsh',
        'auto.offset.reset': 'latest'
    }
)

serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info=row_type_output).build()

kafka_producer = FlinkKafkaProducer(
    topic="output-signal",
    serialization_schema=serialization_schema,
    producer_config={
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'pyflink-producer-group'
    }
)

wm_strategy = (WatermarkStrategy
               .for_bounded_out_of_orderness(Duration.of_seconds(1))
               .with_timestamp_assigner(MyTimestampAssigner()))

ds = (env
      .add_source(kafka_consumer)
      .assign_timestamps_and_watermarks(wm_strategy))

# Process stream continuously with KeyedProcessFunction
calculated_stream = (ds
                     .key_by(lambda x: "all_users", key_type=Types.STRING())
                     .process(JaccardSimilarity(), output_type=row_type_output))

# Print results
calculated_stream.add_sink(kafka_producer)

print("Starting Continuous LSH Flink job - Check console for DEBUG output...")
env.execute("Continuous User Similarity (LSH)")