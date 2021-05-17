CREATE KEYSPACE scheduler WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE scheduler.commands (
    command_id text PRIMARY KEY,
    batch_id text,
    data blob,
    completed boolean,
    succeed boolean
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batches (
    id text PRIMARY KEY,
    completed boolean,
    workers set<text>,
    votes set<text>,
    failures set<text>,
    successes set<text>,
    writes_completed int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.partition_meta (
    topic text,
    p int,
    last_offset bigint,
    PRIMARY KEY (topic, p)
) WITH CLUSTERING ORDER BY (p ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batch_partitions (
    batch_id text,
    command_id text,
    scheduler int,
    PRIMARY KEY (batch_id, command_id, scheduler)
) WITH CLUSTERING ORDER BY (command_id ASC, scheduler ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.log_offsets (
    consumer text PRIMARY KEY,
    offset bigint
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batch_keys (
    batch_id text,
    scheduler int,
    data blob,
    PRIMARY KEY (batch_id, scheduler)
) WITH CLUSTERING ORDER BY (scheduler ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.scheduler_commands (
    batch_id text,
    scheduler int,
    command_id text,
    data blob,
    PRIMARY KEY (batch_id, scheduler, command_id)
) WITH CLUSTERING ORDER BY (scheduler ASC, command_id ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.data (
    id text PRIMARY KEY,
    value int,
    version text
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.topics (
    id text PRIMARY KEY,
    state blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};