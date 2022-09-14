package TotalQuanityOfEachProductSold;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceQuantity extends Reducer<Text, LongWritable, Text, LongWritable> {

    Long totalQuantity = 0L;

    LongWritable result = new LongWritable();

    public void reduce(Text t_key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        System.out.println("in reduce");
        try {



            for (LongWritable row : values) {

                if (row.get() > 0) {
                    totalQuantity += row.get();
                }
            }

            result.set(totalQuantity);


            context.write(t_key, result);
            totalQuantity = 0L;
            System.out.println(result);


        } catch (Exception e) {
            System.out.println(e);
        }
    }
}