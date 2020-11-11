CREATE TABLE IF NOT EXISTS test_logs (
    date date DEFAULT toDate (event_date),
    event_date DateTime ('Asia/Vladivostok'),
    ip_abon IPv4,
    ip_nat IPv4,
    id_nat_list Int32,
    url String,
    ip_server IPv4,
    protocol_type UInt8 
    )
    ENGINE = MergeTree ()
PARTITION BY toMonday (date)
ORDER BY
    (event_date, ip_abon)



CREATE TABLE test.nat_logs (date date DEFAULT toDate (event_date), event_date DateTime, ip_abon String, ip_nat String, id_nat_list Int32, url String, url_reg String DEFAULT replaceRegexpOne (url, '[?](.)+', ''), ip_server String, uid UInt32, tariff UInt16, s days_in_block UInt16, block_type UInt8) ENGINE = ReplicatedMergeTree ('/clickhouse/tables/{shard}/test/nat_logs_replicated_bc', '{replica}')
PARTITION BY toMonday (date)
ORDER BY
    (uid, ip_abon, event_date, intHash32 (id_nat_list)) SAMPLE BY
        intHash32 (id_nat_list) SETTINGS index_granularity = 8192 

@@@ @@@ @
INSERT INTO test_logs (event_date, ip_abon, ip_nat, id_nat_list, url, ip_server)
VALUES ('2020-11-10 16:06:00', '10.15.193.106', '109.126.4.154', '15', 'https://ad.mail.ru', '94.100.180.197:443')
