package commonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriends {

    private static class CommonFriendsMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private int x;

        @Override
        protected void setup(Mapper<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            x = conf.getInt("x", 0);
        }

        @Override
        protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        }
    }

    private static class CommonFriendsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        }
    }

    private static class CommonFriendsDriver{
        public static void main(String[] args) throws IOException {
            Configuration conf = new Configuration();
            conf.set("x", "15");

            Job job = Job.getInstance(conf, "CommonFriendsJob");
        }
    }
}
