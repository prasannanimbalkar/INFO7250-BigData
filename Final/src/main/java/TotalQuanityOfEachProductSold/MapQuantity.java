package TotalQuanityOfEachProductSold;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapQuantity extends Mapper<Object, Text, Text, LongWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map");
        try{
            String valueString = value.toString();
            String[] items = valueString.split(",");

            String productId = items[2];
            Long Quantity = 0L;

            Quantity = Long.parseLong(items[3]);


            LongWritable outputValue = new LongWritable(Quantity);
            context.write(new Text(productId), outputValue);

        }catch (Exception e){
            System.out.println("error "+e);
        }

    }
}
