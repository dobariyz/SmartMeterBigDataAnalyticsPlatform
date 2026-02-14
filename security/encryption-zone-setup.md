hadoop --daemon start kms
hdfs crypto -createZone -keyName houseInfoKey -path /HouseInfoEncrypted
hdfs crypto -getFileEncryptionInfo /HouseInfoEncrypted/House_Info.csv
