config = {
    'kafka_broker': ['localhost:9092'], # (can be a list for multiple: ['host1:9092', 'host2:9092'])
    'topic_name': 'uber-trips-raw',
    'vehicles_per_province': {'corrientes': 50},
    'duration_seconds': 120,
}

# To simulate with many vehicles across different provinces (ensure POI files exist):
# config = {
#     ...
#     'vehicles_per_province': {'corrientes': 10, 'buenos_aires': 15, 'mendoza': 5},
#     ...
# }