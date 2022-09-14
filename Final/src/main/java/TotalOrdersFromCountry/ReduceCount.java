package TotalOrdersFromCountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceCount extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text t_key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long frequencyForCountry = 0L;
        LongWritable result = new LongWritable();
        try {

            for (LongWritable row : values) {
                frequencyForCountry++;

            }
            System.out.println(frequencyForCountry);

            result.set(frequencyForCountry);
            context.write(t_key, result);

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}