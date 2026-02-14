package GroupPhase3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunMaxHourlyJob {

    public static void main(String[] args) throws Exception {

        // Check command line arguments
        if (args.length != 2) {
            System.err.println("Usage: RunMaxHourlyJob <input path> <output path>");
            System.err.println("Example: RunMaxHourlyJob /dataset/consumption/2016_output/ /output/max_hourly");
            System.exit(-1);
        }

        // Create configuration
        Configuration conf = new Configuration();

        // Create job
        Job job = Job.getInstance(conf, "Maximum Hourly Electricity Consumption");
        job.setJarByClass(RunMaxHourlyJob.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MaxHourlyMapper.class);
        job.setReducerClass(MaxHourlyReducer.class);

        // Set output key and value types for Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set output key and value types for Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set number of reduce tasks (1 to ensure single global maximum)
        job.setNumReduceTasks(1);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
