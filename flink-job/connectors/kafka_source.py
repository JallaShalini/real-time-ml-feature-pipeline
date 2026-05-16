from kafka import KafkaConsumer

def create_consumer(topic, bootstrap_servers='kafka:9092', group_id=None):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id, auto_offset_reset='earliest', enable_auto_commit=True)
    return consumer
# Placeholder Kafka source connector definitions for PyFlink
def create_user_events_source(env, topic, bootstrap_servers):
    # This function would create a Kafka consumer source for 'user-events'
    pass

def create_metadata_table(env, topic, bootstrap_servers):
    # This function would create a Table backed by the compacted metadata topic
    pass
