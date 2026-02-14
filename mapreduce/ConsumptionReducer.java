package GroupPhase2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;

public class ConsumptionReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String datePath = key.toString();

        String header = "LOG_ID\tHOUSE_ID\tCONDATE\tCONHOUR\tENERGY_READING\tFLAG";
        multipleOutputs.write(NullWritable.get(), new Text(header), datePath + "/data");

        for (Text value : values) {
            multipleOutputs.write(NullWritable.get(), value, datePath + "/data");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
