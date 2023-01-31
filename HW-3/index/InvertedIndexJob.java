import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class InvertedIndexJob {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text id_doc = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] doc = value.toString().split("\t", 2);
            id_doc.set(doc[0]);

            String buffer_text = doc[1].toLowerCase();
            buffer_text = buffer_text.replaceAll("[^a-z\\s]", " ");
            buffer_text = buffer_text.replaceAll("\\s+", " ");


            StringTokenizer tokenizer = new StringTokenizer(buffer_text);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, id_doc);
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text return_answer = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> count = new HashMap<>();
            for (Text val : values) {
                String id_doc = val.toString();
                count.put(id_doc, count.getOrDefault(id_doc, 0) + 1);
            }

            StringBuilder s = new StringBuilder();
            for (String k : count.keySet())
                s.append(k).append(":").append(count.get(k)).append("\t");

            return_answer.set(s.substring(0, s.length() - 1));
            context.write(key, return_answer);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
