CREATE EXTERNAL TABLE consumption_raw (
    log_id INT,
    house_id INT,
    condate STRING,
    conhour STRING,
    energy_reading DOUBLE,
    flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/dataset/consumption/meter_data/raw/';
