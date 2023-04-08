import java.io.IOException;
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

import static java.lang.Math.min;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;

public class TopkCommonWords {

    /*
    Mapper class to remove stopwords and check length of each word is more than 4 from both files.
    Also maps words to the files they belong to
    */
    public static class TopkCommonWordsMapper
            extends Mapper<Object, Text, Text, Text> {

        public static final int MIN_WORDS = 4;
        public Set<String> stopWords = new HashSet<>();

        private final static Text file = new Text();
        private Text word = new Text();
        
        //Override original set up to write to hashset the individual stop words in stopwords.txt
        @Override
        protected void setup(Context context
                         ) throws IOException, InterruptedException {

            URI[] cacheFiles = context.getCacheFiles();
            Path stopWordsPath = new Path(cacheFiles[0]); //stopwords.txt

            Configuration conf = context.getConfiguration();

            FileSystem myFilesystem = FileSystem.get(context.getConfiguration());
            BufferedReader myBuffReader = new BufferedReader(new InputStreamReader(myFilesystem.open(stopWordsPath)));
            String stopWord = null;
            while ((stopWord = myBuffReader.readLine()) != null) {
                stopWords.add(stopWord);
            }    
        }

        /* Since we need to compare how many times a word has appeared in a file
        We may be tempted to partition the content with the file name as the key
        However, the opposite approach where the word is the key was actually 
        better in this implementation

        The map function reads the two files and emits a pair where the word is the key
        And the file name is the value. It also only emits them if the word is more 
        than 4 letters long and is not a stop word
        */
        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {

            String[] tokenizeInput = value.toString().trim().split("\\s+|\\t|\\n|\\r|\\f");

            for (int i = 0; i < tokenizeInput.length; i++) {

                //Emit only if it is not stop word and is of sufficient length
                if (!stopWords.contains(tokenizeInput[i].toString()) && tokenizeInput[i].toString().length()>MIN_WORDS) {
                    
                    word.set(tokenizeInput[i]);

                    //Splits by file name
                    FileSplit fileSplit = (FileSplit)context.getInputSplit();
                    String filename = fileSplit.getPath().getName();
                    file.set(filename);
                    context.write(word, file);
                }  
            }
        }
    }


    /*
    Reducer class handles the counting and ordering of different words and outputs top K words
    */
    public static class TopkCommonWordsReducer
                    extends Reducer<Text, Text, Integer, Text> {

        Map<Text, Integer> reducerEmit = new HashMap<>();

        /* 
        In the reduce method, we count how many times a word appears in a file, then we find the lower of the 
        two, if a word does not exist in any of the two files, we disregard it as it is not a common word
        Otherwise, we emit the word as a key and the lower bound of the frequency of words as the value
        */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            URI[] cacheFiles = context.getCacheFiles();
            Path input1File = new Path(cacheFiles[1]); // input 1
            Path input2File = new Path(cacheFiles[2]); // input 2

            int numfile1 = 0;
            int numfile2 = 0;
            int lowerBound = 0;

            /*
            Counts number of times the key, which is a word, appears in the list of files, which is the
            list of values
            */
            for (Text myFileName : values) {

                if (myFileName.toString().equals(input1File.getName())) {
                    numfile1++;
                }
                if (myFileName.toString().equals(input2File.getName())) {
                    numfile2++;
                }
            }

            /*
            If word appears in both the files we emit the word along with the lower bound
            of the number of times the word appears in both files
            */
            if (numfile1*numfile2 != 0) {
                Text word = new Text(key);
                lowerBound = min(numfile1,numfile2);
                reducerEmit.put(word,lowerBound); 
            }
        }

        /*
        We override the cleanup method to Sort keys and values emitted by the reducer.
        We first order the frequency in descending values. Thereafter, for words
        with same frequency we order them lexicographically. Finally we write out the top k common words 
        */
        @Override
        public void cleanup(Context context) 
                        throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = conf.getInt("myK",0);
            int count = 0;

            LinkedHashMap<Text, Integer> topCommonAndOrderedWords = new LinkedHashMap<>();

            /*
            Add reducerEmit to linked hashmap and sort in descending values of word frequency
            Thereafter, we sort lexicographically
            */
            reducerEmit.entrySet().stream().sorted(Map.Entry.<Text,Integer>comparingByValue().reversed()
                            .thenComparing(Map.Entry.comparingByKey())).forEachOrdered(x -> topCommonAndOrderedWords.
                            put(x.getKey(), x.getValue()));

            //Writes out the sorted top k common words          
            for (Map.Entry<Text,Integer> keyAndValuePair : topCommonAndOrderedWords.entrySet()) {
                if (count < k) {
                    context.write(keyAndValuePair.getValue(),keyAndValuePair.getKey());
                    count++;
                }
                else {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.setInt("myK",Integer.parseInt(args[4])); // k  
        Job job = Job.getInstance(conf, "top K common words");

        job.setJarByClass(TopkCommonWords.class);

        job.addCacheFile(new Path(args[2]).toUri()); // stopwords
        job.addCacheFile(new Path(args[0]).toUri());
        job.addCacheFile(new Path(args[1]).toUri());

        job.setMapperClass(TopkCommonWordsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TopkCommonWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //input 1
        FileInputFormat.addInputPath(job, new Path(args[1])); // input 2
        FileOutputFormat.setOutputPath(job, new Path(args[3])); //output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
