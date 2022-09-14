package TotalAmountOfOrderMadeByUser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceProduct extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text result = new Text();
        String[] mapData;
//        System.out.println("in reduce");
        try {
            double totalRounded = 0;
            for (Text row : values) {
                mapData = row.toString().split(",");
                if(Double.parseDouble(mapData[0]) >= 0) {
                    double total = Double.parseDouble(mapData[0]) * Double.parseDouble(mapData[1]);
                    totalRounded = Math.round(total * 100D) / 100D;
                }
                result.set(String.valueOf(totalRounded));
                context.write(t_key, result);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}