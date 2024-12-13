import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "WordCount");
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(FriendMap.class);
      job.setReducerClass(FriendReduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class FriendMap extends Mapper<LongWritable, Text, Text, Text> {
      private Text outputKey = new Text();
      private Text outputVal = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

         String[] tokens = value.toString().split("\t");
         if (tokens.length != 2) return; 

         String user = tokens[0];
         String[] friends = tokens[1].split(",");

         for (String friend : friends) {
            outputKey.set(user);
            outputVal.set("FRIEND_" + friend);
            context.write(outputKey, outputVal);
            outputKey.set(friend);
            outputVal.set("FRIEND_" + user);
            context.write(outputKey, outputVal);
         }
         int friendsLength = friends.length;
         for (int i = 0; i < friendsLength; i++) {
            outputKey.set(friends[i]);
            for (int j = i + 1; j < friendsLength; j++) {
               outputVal.set(friends[j]);
               context.write(outputKey, outputVal);
               context.write(outputVal, outputKey);
            }
       }

      }
   }

   public static class FriendReduce extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

               Set<String> actualFriends = new HashSet<>();
               Map<String, Integer> recommendations = new HashMap<>();

               for (Text value : values) {
                  String friend = value.toString();
       
                  if (friend.startsWith("FRIEND_")) {
                     actualFriends.add(friend.substring(7));
                  } else {
                     recommendations.put(friend, recommendations.getOrDefault(friend, 0) + 1);
                  }
               }
       
               for (String actualFriend : actualFriends) {
                   recommendations.remove(actualFriend);
               }

               List<Map.Entry<String, Integer>> sortedRecommendations = new ArrayList<>(recommendations.entrySet());
               sortedRecommendations.sort((a, b) -> {
                  int compareMutualCount = Integer.compare(b.getValue(), a.getValue());
                  if (compareMutualCount != 0) {
                     return compareMutualCount;
                  } else {
                     return Integer.compare(Integer.parseInt(a.getKey()), Integer.parseInt(b.getKey()));
                  }
               });

               List<String> topRecommendations = new ArrayList<>();
               for (int i = 0; i < Math.min(10, sortedRecommendations.size()); i++) {
                   topRecommendations.add(sortedRecommendations.get(i).getKey());
               }

               context.write(key, new Text(String.join(",", topRecommendations)));

      }
   }
}