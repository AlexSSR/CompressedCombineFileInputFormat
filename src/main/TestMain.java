package main;
import java.io.IOException;

import com.hadoop.compression.lzo.LzopCodec;
import main.java.input.CompressedCombineFileInputFormat;
import main.java.input.CompressedCombineFileWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TestMain extends Configured implements Tool{

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TestMain (), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();

//    //map的压缩输出
//    conf.setBoolean("mapred.compress.map.out", true);//设置map输出压缩
//    conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, LzopCodec.class, CompressionCodec.class);


        Job job = new Job(conf);
        job.setJobName("CombineFile");
        job.setJarByClass(TestMain.class);

        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);

        //将文件作为原始输入划分成一个切片
        job.setInputFormatClass(CompressedCombineFileInputFormat.class);


        //设置mapoutput输出类
        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输出大小
        CompressedCombineFileInputFormat.setMaxInputSplitSize(job, 1024*1024*128);

        //设置reducetask为 0
        job.setNumReduceTasks(0);

        //定义输出路径，输入路径
        CompressedCombineFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
        job.waitForCompletion(true);

        return 0;
    }

    public static class TestMapper extends Mapper<CompressedCombineFileWritable, Text, Text, NullWritable>{

        NullWritable v = NullWritable.get();

        @Override
        public void map (CompressedCombineFileWritable key, Text value, Context context) throws IOException, InterruptedException{
            context.write(value, v);
        }
    }

}
