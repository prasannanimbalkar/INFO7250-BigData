package TotalAmountForInvoice;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapSum extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println("map 2");
        try {

            String valueString = value.toString();
            String[] reducedData = valueString.split("\t");
            System.out.println(reducedData[0]);
            Text Product = new Text(reducedData[1]);
            context.write(new Text(reducedData[0]), Product);

        }catch(Exception e){
            System.out.println("error "+ e);
        }
    }
}
