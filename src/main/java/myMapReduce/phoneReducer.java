package myMapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class phoneReducer extends Reducer<Text, myMapReduce.flowBean,Text, myMapReduce.flowBean>{
    myMapReduce.flowBean flowBean = new flowBean();
    @Override
    protected void reduce(Text key , Iterable<myMapReduce.flowBean>  FlowBeans , Context context)
            throws IOException, InterruptedException {
        long upSumFlow = 0;
        long downSumFlow =0;
        for (myMapReduce.flowBean bean : FlowBeans) {
            upSumFlow += bean.getUpFlow();
            downSumFlow += bean.getDownFlow();
        }
        flowBean.set(upSumFlow,downSumFlow);
        context.write(key,flowBean);
    }
}
