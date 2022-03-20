
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kerby.config.Conf;
import org.eclipse.jetty.xml.ConfigurationProcessor;

import java.io.IOException;
import java.util.Iterator;

public class cellphoneTraffic {

    public static class TrafficMapper
            extends Mapper<Object, Text, Text, phoneTraffic>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            if (lines.length < 10) {
                return;
            }

            String phone = lines[1];
            try {
                long upStream = Long.parseLong(lines[8]);
                long downStream = Long.parseLong(lines[9]);
                context.write(new Text(phone),
                        new phoneTraffic(upStream, downStream, upStream + downStream));
            } catch (NumberFormatException e) {
                System.err.println("parseLong failed: " + e.getMessage());
            }
        }
    }

    public static class TrafficReducer
            extends Reducer<Text,phoneTraffic,Text,phoneTraffic> {


        public void reduce(Text key, Iterable<phoneTraffic> values,
                           Context context
        ) throws IOException, InterruptedException {
            long totalUp = 0;
            long totalDown = 0;
            long totalSum = 0;
            for(phoneTraffic val: values) {
                totalUp += val.getUpstream();
                totalDown += val.getDownstream();
                totalSum += val.getSum();
            }

            context.write(key, new phoneTraffic(totalUp, totalDown, totalSum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration  conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: cellphoneTraffic <int> <out>");
            System.exit(2);
        }
        System.out.println("otherArgs: " + Arrays.toString(otherArgs));

        Job job = Job.getInstance(conf, "cellphoneTraffic");
        job.setJarByClass(cellphoneTraffic.class);
        job.setMapperClass(TrafficMapper.class);
        job.setCombinerClass(TrafficReducer.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(phoneTraffic.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length-2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true) ? 0:1);

    }
}
