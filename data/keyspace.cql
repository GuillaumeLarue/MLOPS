CREATE
KEYSPACE mlops WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
                       AND durable_writes = true;