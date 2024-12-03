import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        
        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(conf);
        
        if (fs.exists(outputPath)) {
            System.err.println("Output path already exists: " + otherArgs[1]);
            System.exit(1);
        }

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Optional: Enable Combiner for performance
        job.setCombinerClass(MyReducer.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        private static final Pattern pattern = Pattern.compile("[A-Za-z0-9]+|\\.|\\-|\\[|\\]|\\(|\\)|,");
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            while (matcher.find()) {
                word.set(matcher.group());
                context.write(word, one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
