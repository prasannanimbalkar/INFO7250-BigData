package TotalAmountForInvoice;

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

//    Job 1
    Job job = Job.getInstance(configuration, "Data");
    job.setJarByClass(Driver.class);

    Job job2 = Job.getInstance(configuration, "Data2");
    job2.setJarByClass(Driver.class);

    job.setMapperClass(MapProduct.class);
    job.setReducerClass(ReduceProduct.class);

    //output of the mapper
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    FileInputFormat.setInputPaths(job, "/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/Data/onlineRetailMain.csv");
    FileOutputFormat.setOutputPath(job, new Path("/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/Output/TotalAmountForInvoice/OutputReduceOne"));


    if(job.waitForCompletion(true)){
        job2.setMapperClass(MapSum.class);
        job2.setReducerClass(ReduceSum.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, "/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/Output/TotalAmountForInvoice/OutputReduceOne/part-r-00000");
        FileOutputFormat.setOutputPath(job2, new Path("/Users/prasannanimbalkar/Desktop/MSIS/BigData/FinalProject/Output/TotalAmountForInvoice/TotalAmountForInvoice_Final/"));
    }

    System.exit(job2.waitForCompletion(true) ? 0 : 1);


    }
}