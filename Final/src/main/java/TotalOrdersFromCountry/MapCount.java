package TotalOrdersFromCountry;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class MapCount extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] InvoiceData = valueString.split(",");
        context.write(new Text(InvoiceData[7]), one);
    }
}
