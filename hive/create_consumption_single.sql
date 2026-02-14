CREATE EXTERNAL TABLE consumption_single (
    log_id INT,
    house_id INT,
    condate STRING,
    conhour STRING,
    energy_reading DOUBLE,
    flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/dataset/consumption/meter_data/single_file/'
TBLPROPERTIES ("skip.header.line.count"="1");
