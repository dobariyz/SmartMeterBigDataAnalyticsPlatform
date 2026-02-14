sudo adduser houseinfo_reader
hadoop fs -mkdir /HouseInfo
hadoop fs -chown houseinfo_reader /HouseInfo
hadoop fs -chmod 500 /HouseInfo
