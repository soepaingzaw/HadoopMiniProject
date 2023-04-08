# Hadoop Word Comparison and Retrieval Mini Project

This mini project is a simple demonstration of how to use Hadoop to compare the content between two files, remove stopwords and retrieve the top k most common words of sufficient length in the file

### Prerequisites
 - Hadoop (version 2.7.3 or higher)
 - Java (version 1.8 or higher)


### Bash File Configuration

The following bash file serves as a guide, it helps by:
1. Deleting previous files
2. Uploading input files
3. Setting the top k words to output (i.e. 10)
4. Submitting the job to run


```
hadoop com.sun.tools.javac.Main TopkCommonWords.java
jar cf cm.jar TopkCommonWords*.class
hdfs dfs -rm -r ./temp/
hdfs dfs -mkdir -p ./temp/
hdfs dfs -mkdir ./temp/input/
hdfs dfs -put ./data/* ./temp/input/
k = 10
hadoop jar cm.jar TopkCommonWords ./temp/input/input1.txt ./temp/input/input2.txt ./temp/input/stopwords.txt ./temp/output/ $k
```
All of these are managed by the Hadoop Distributed File System (HDFS)

## Input Files

The input files should be plain text files, and can be located in any directory accessible to the Hadoop cluster. The files provided in `data` are examples that you can use

## Stopwords File
Stop words are the words which are filtered out before or after processing of data because they are insignificant due to their excessive prevalence in documents. The stopwords list is provided in `data`. Examples of stopwords include `The`, `I`, `a`, etc . . . 


## Output Files
The output of the Hadoop job will be a list of the most common K words in both files, sorted by frequency and then lexicographically