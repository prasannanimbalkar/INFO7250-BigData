package Counter2.Counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
	public static void main(String[] args) throws Exception {

    Configuration configuration = new Configuration();

    Job job = Job.getInstance(configuration, "Data");
    job.setJarByClass(Driver.class);

    job.setMapperClass(MapProduct.class);
    job.setReducerClass(ReduceProduct.class);

    //output of the mapper
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    FileInputFormat.setInputPaths(job, "/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/Data/testdata.csv");
    FileOutputFormat.setOutputPath(job, new Path("/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/out14"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}