from websocket_server import feature_cache

def get_entity_features(entity_id):
    return feature_cache.get(entity_id, {})
