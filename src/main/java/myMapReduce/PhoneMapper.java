package myMapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
    Text tKey = new Text();
    @Override
    protected void map(LongWritable key , Text value , Context context )
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        String phoNum = fileds[1];
        long upFlow = Long.parseLong(fileds[fileds.length-3]);
        long downFlow = Long.parseLong(fileds[fileds.length-2]);
        tKey.set(phoNum);
        FlowBean flowBean = new FlowBean(upFlow,downFlow);
        context.write(tKey,flowBean);
    }
}
