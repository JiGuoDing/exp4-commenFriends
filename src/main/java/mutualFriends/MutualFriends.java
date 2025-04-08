package mutualFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MutualFriends {
    /**
     * 生成有序用户对
     */
    public static class MutualFriendsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private final Text pair = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");

            // 当前用户
            String user = line[0];
            // 所有关注的好友
            String[] friends = line[1].trim().split("\\s+");

            // 为每个好友生成有序对
            // 写出用户的关注列表，确保 id 较小的用户在前面，即按字典序排序用户对
            for (String friend : friends) {
                if (friend.isEmpty())
                    continue;
                if (user.compareTo(friend) < 0)
                    pair.set(user+"-"+friend);
                else
                    pair.set(friend+"-"+user);
                // 输出键值对，例如 <"1-100", 1>
                context.write(pair, one);
            }
        }
    }

    /**
     * 统计互相关注对
     */
    public static class MutualFriendsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // <user-friend>出现两次说明user与friend互相关注
            if (sum == 2){
                context.write(key, new IntWritable(1));
            }
        }
    }

    public static class MutualFriendsDriver{
        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MutualFriends");
            job.setJarByClass(MutualFriends.class);
            job.setMapperClass(MutualFriendsMapper.class);
            job.setReducerClass(MutualFriendsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
