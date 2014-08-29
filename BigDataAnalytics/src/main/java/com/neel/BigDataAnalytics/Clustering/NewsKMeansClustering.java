package com.neel.BigDataAnalytics.Clustering;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import com.neel.BigDataAnalytics.Utils.MyAnalyzer;

public class NewsKMeansClustering {
  
  public static void main(String args[]) throws Exception {
    
    int minSupport = 5;
    int minDf = 5;
    int maxDFPercent = 99;
    int maxNGramSize = 1;
    int minLLRValue = 50;
    int reduceTasks = 1;
    int chunkSize = 200;
    int norm = -1;
    boolean sequentialAccessOutput = true;
    
    String inputDir = "hdfs://localhost:54310/Classification/20_newgroups_seq/";
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://localhost:54310/");
    FileSystem fs = FileSystem.get(conf);

    String outputDir = "hdfs://localhost:54310/Clustering";
    HadoopUtil.delete(conf, new Path(outputDir));
    Path tokenizedPath = new Path(outputDir,
        DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
    MyAnalyzer analyzer = new MyAnalyzer();
    DocumentProcessor.tokenizeDocuments(new Path(inputDir), analyzer
        .getClass().asSubclass(Analyzer.class), tokenizedPath, conf);

    DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
      new Path(outputDir), DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
      conf, minSupport, maxNGramSize, minLLRValue, -1.0f, true, reduceTasks,
      chunkSize, sequentialAccessOutput, false);
    Pair<Long[], List<Path>> dfData = TFIDFConverter.calculateDF(
    		new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
    	    new Path(outputDir), conf, chunkSize);   
    TFIDFConverter.processTfIdf(
      new Path(outputDir , DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
      new Path(outputDir), conf, dfData, minDf,
      maxDFPercent, norm, true, sequentialAccessOutput, false, reduceTasks);
    Path vectorsFolder = new Path(outputDir, "tfidf-vectors");
    Path centroids = new Path(outputDir, "centroids");
    Path clusterOutput = new Path(outputDir, "clusters");
    
  RandomSeedGenerator.buildRandom(conf, vectorsFolder, centroids, 20,
      new CosineDistanceMeasure());
    KMeansDriver.run(conf, vectorsFolder, centroids, clusterOutput,
      new CosineDistanceMeasure(), 0.01, 20, true, 0, false);
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        //new Path(clusterOutput, Cluster.CLUSTERED_POINTS_DIR+ "/part-m-00000")
        new Path("hdfs://localhost:54310/Classification/20_newgroups_Cluster/tfidf-vectors/part-r-00000")
       , conf);
   // IntWritable key =  new IntWritable();
    Text key =  new Text();

    VectorWritable value = new VectorWritable();
    while (reader.next(key, value)) {
       // System.out.println(key.get() + " | " + value.get());
        System.out.println(key.toString() + " | " + value.get());
    }
    reader.close();
    
  }
}