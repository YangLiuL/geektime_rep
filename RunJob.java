
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
    public static void main(String[] args ) {
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem. get ( config );
            Job job = Job. getInstance ( config );
            job.setJobName("geektime_bigdata_ly");
            job .setJarByClass(RunJob. class );
            job .setMapperClass(PhoneMapper.class );
            job .setReducerClass(PhoneReducer.class );
            job .setMapOutputKeyClass(Text. class );
            job .setMapOutputValueClass(FlowBean. class );
            FileInputFormat. setInputPaths ( job , new Path( "/user/student/liuyang/input/HTTP_20130313143750.dat" ));
            Path output = new Path( "/user/student/liuyang/input/output" );
            if ( fs .exists( output )){
                fs .delete( output , true );
            }
            FileOutputFormat. setOutputPath ( job , output );
            boolean f = job .waitForCompletion( true );
            if ( f ){
                System. out .println( " 执行成功 " );
            }
        } catch (Exception e ) {
            e .printStackTrace();
        }
    }
}
