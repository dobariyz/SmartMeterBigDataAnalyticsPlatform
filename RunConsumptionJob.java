package GroupPhase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RunConsumptionJob {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: RunConsumptionJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator", "\t");

        Job job = Job.getInstance(conf, "Consumption Data 2015 Organizer");

        job.setJarByClass(RunConsumptionJob.class);
        job.setMapperClass(ConsumptionMapper.class);
        job.setReducerClass(ConsumptionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
