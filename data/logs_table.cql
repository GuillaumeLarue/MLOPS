USE mlops;

CREATE TABLE logs
(
    uuid      uuid PRIMARY KEY,
    sensor_id int,
    type      text,
    value     float,
    time      timestamp,
);

INSERT INTO logs (uuid, sensor_id, time, type, value)
VALUES (uuid(),
        1,
        toTimestamp(now()),
        'temperature',
        20.2);