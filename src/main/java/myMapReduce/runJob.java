package myMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.myUtils;

public class runJob {
    public static void main(String[] args) {
        //设置conf 配置
        Configuration config = new Configuration();
        //读取输入输出路径参数
        myUtils.readProperties("geektime.properties");
        String inputFile =myUtils.getProperty("hadoop.InputFilePath");
        String outFile = myUtils.getProperty("hadoop.OutputFilePath");
        String jobName =myUtils.getProperty("geektime.StduentNum");
//        System.out.println(inputFile);
//        System.out.println(outFile);
        try {
            FileSystem fileSystem = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName(jobName);
            job.setJarByClass(runJob.class);
            job.setMapperClass(phoneMapper.class);
            job.setReducerClass(phoneReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(flowBean.class);
            FileInputFormat.setInputPaths(job, new Path(inputFile));
            Path output = new Path(outFile);
            if (fileSystem.exists(output)) {
                fileSystem.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);
            if (job.waitForCompletion(true)) {
                System.out.println(" 执行成功 ");
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
