USE mlops;

CREATE TABLE sensor_type_list
(
    id          int PRIMARY KEY,
    name        text,
    description text,
    unit        text,
    min_value   int,
    max_value   int,
);

INSERT INTO sensor_type_list (uuid, description, max_value, min_value, name, unit)
VALUES (uuid(),
        'Temperature sensor',
        100,
        -100,
        'temperature',
        '°C');