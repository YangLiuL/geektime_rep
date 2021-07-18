import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key , Iterable<FlowBean>  FlowBeans ,Context context)
            throws IOException, InterruptedException {
        long upSumFlow = 0;
        long downSumFlow =0;
        for (FlowBean bean : FlowBeans) {
            upSumFlow += bean.getUpFlow();
            downSumFlow += bean.getDownFlow();
        }
        flowBean.set(upSumFlow,downSumFlow);
        context.write(key,flowBean);
    }
}
