package TotalOrdersFromCountry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
	public static void main(String[] args) throws Exception {

    Configuration configuration = new Configuration();

    Job job = Job.getInstance(configuration, "Data");
    job.setJarByClass(Driver.class);

    job.setMapperClass(MapCount.class);
    job.setReducerClass(ReduceCount.class);

    //output of the mapper
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);


    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    FileInputFormat.setInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}