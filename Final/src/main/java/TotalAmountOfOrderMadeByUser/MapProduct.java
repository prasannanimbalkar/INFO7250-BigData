package TotalAmountOfOrderMadeByUser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapProduct extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println("map 1");
        try{
            String valueString = value.toString();
            String[] InvoiceData = valueString.split(",");
            Text outputValue = new Text(InvoiceData[3] + "," +InvoiceData[5]);
            System.out.println("key " +new Text(InvoiceData[6]) + "value "+ outputValue);
            if(InvoiceData[6] != ""){
                context.write(new Text(InvoiceData[6]), outputValue);
            }


        }catch (Exception e){
            System.out.println("error "+e);
        }

    }
}
