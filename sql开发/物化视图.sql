-- 只能针对汇总(sum),计数(count)
CREATE MATERIALIZED VIEW download_daily_mv
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day) ORDER BY (userid, day)
POPULATE
AS SELECT
  toStartOfDay(when) AS day,
  userid,
  count() as downloads,
  sum(bytes) AS bytes
FROM download
GROUP BY userid, day;


--针对任意汇总

CREATE TABLE counter_daily (
  day DateTime,
  device UInt32,
  count UInt64,
  max_value_state AggregateFunction(max, Float32),
  min_value_state AggregateFunction(min, Float32),
  avg_value_state AggregateFunction(avg, Float32)
)
ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (device, day)

CREATE MATERIALIZED VIEW counter_daily_mv
TO counter_daily
AS SELECT
    toStartOfDay(when) as day,
    device,
    count(*) as count,
    maxState(value) AS max_value_state,
    minState(value) AS min_value_state,
    avgState(value) AS avg_value_state
FROM counter
WHERE when >= toDate('2019-01-01 00:00:00')
GROUP BY device, day
ORDER BY device, day
The TO keyword lets us point t
