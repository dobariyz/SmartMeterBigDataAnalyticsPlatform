package GroupPhase2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ConsumptionMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            String line = value.toString();

            if (line.startsWith("LOG_ID") || line.trim().isEmpty()) {
                return;
            }

            String[] fields = line.split("\t");

            if (fields.length >= 6) {
                String logId = fields[0].trim();
                String houseId = fields[1].trim();
                String condate = fields[2].trim();
                String conhour = fields[3].trim();
                String energyReading = fields[4].trim();
                String flag = fields[5].trim();

                String year = condate.substring(0, 4);

                if (year.equals("2015")) {
                    String month = condate.substring(5, 7);
                    String day = condate.substring(8, 10);

                    String dateKey = year + "/" + month + "/" + day;

                    String rowData = logId + "\t" + houseId + "\t" + condate + "\t" +
                            conhour + "\t" + energyReading + "\t" + flag;

                    context.write(new Text(dateKey), new Text(rowData));
                }
            }
        } catch (Exception e) {
        }
    }
}
