package GroupPhase3;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MaxHourlyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            String line = value.toString().trim();

            // Skip header lines and empty lines
            if (line.isEmpty() || line.startsWith("LOG_ID")) {
                return;
            }

            // Split by tab or comma (adjust based on your file format)
            String[] fields = line.split("\t");

            // Check if we have all required fields
            if (fields.length >= 5) {
                String houseId = fields[1].trim();
                String condate = fields[2].trim();
                String conhour = fields[3].trim();
                String energyReadingStr = fields[4].trim();

                // Parse energy reading
                double energyReading = Double.parseDouble(energyReadingStr);

                // Create composite key: houseId_date_hour
                // Example: "19_2016-08-20_00"
                String compositeKey = houseId + "" + condate + "" + conhour;

                // Emit the composite key with the energy reading
                context.write(new Text(compositeKey), new DoubleWritable(energyReading));
            }

        } catch (NumberFormatException e) {
            // Skip malformed records
            System.err.println("Error parsing line: " + value.toString());
        } catch (Exception e) {
            // Skip any other errors
            System.err.println("Unexpected error: " + e.getMessage());
        }
    }
}
