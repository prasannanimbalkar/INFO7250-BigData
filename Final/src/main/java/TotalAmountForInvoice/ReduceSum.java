package TotalAmountForInvoice;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceSum extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text resultSum = new Text();
//        System.out.println("in reduce 2");
        try {
            double sum = 0;
            double totalSum = 0;
            for (Text rows : values) {
                sum += Double.parseDouble(rows.toString());
                totalSum = Math.round(sum * 100D) / 100D;
            }

            resultSum.set(String.valueOf(totalSum));
            context.write(t_key, resultSum);

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}