
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key , Text value , Context context )
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        String phoNum = fileds[1];
        long upFlow = Long.parseLong(fileds[fileds.length-3]);
        long downFlow = Long.parseLong(fileds[fileds.length-2]);
        k.set(phoNum);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }
}
