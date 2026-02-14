package GroupPhase3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class MaxHourlyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // Track the global maximum consumption across all hours
    private double globalMaxConsumption = 0.0;
    private String maxConsumptionKey = "";

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double minReading = Double.MAX_VALUE;
        double maxReading = Double.MIN_VALUE;

        // Find minimum and maximum readings for this hour
        for (DoubleWritable value : values) {
            double reading = value.get();

            if (reading < minReading) {
                minReading = reading;
            }
            if (reading > maxReading) {
                maxReading = reading;
            }
        }

        // Calculate hourly consumption (cumulative readings, so max - min)
        double hourlyConsumption = maxReading - minReading;

        // Update global maximum if this hour's consumption is higher
        if (hourlyConsumption > globalMaxConsumption) {
            globalMaxConsumption = hourlyConsumption;
            maxConsumptionKey = key.toString();
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the maximum hourly consumption found across all data
        context.write(
                new Text("Maximum Hourly Consumption (kWh) at " + maxConsumptionKey),
                new DoubleWritable(globalMaxConsumption)
        );

        // Also output just the value for easy reference
        context.write(
                new Text("Maximum Value"),
                new DoubleWritable(globalMaxConsumption)
                );
    }
}
