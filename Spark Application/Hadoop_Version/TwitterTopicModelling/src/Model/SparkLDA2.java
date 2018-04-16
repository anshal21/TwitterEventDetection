/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import javafx.util.Pair;
import org.apache.derby.iapi.sql.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LDAOptimizer;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;



/**
 *
 * @author anshal
 */
public class SparkLDA2 {

    /**
     * @param args the command line arguments
     */
    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;
    private int topicCount;
    private int termPerTopic;
    String outputFile;
    Map<String,Integer> dict;
    
    public SparkLDA2(SparkSession sparkSession, int topicCount,int termPerTopic,String outputFile){
        this.sparkSession = sparkSession;
        this.topicCount = topicCount;
        this.termPerTopic = termPerTopic;
        this.outputFile = outputFile;
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }
   
    public class TopicTerm{
        public String word;
        public double prob;
    }
    class comp implements Comparator<TopicTerm>{
        @Override
        public int compare(TopicTerm o1, TopicTerm o2) {
           if((o1.prob - o2.prob)<0) return 1;
           else return -1;
        }
    }
    public void fit() throws IOException {
        long start = System.currentTimeMillis();
        JavaRDD<String> data = sparkContext.textFile("hdfs://"+outputFile+"input.LDA");
        JavaRDD<Vector> parsedData = data.map(s->{
           String[] sarray = s.trim().split(" ");
           double[] values = new double[sarray.length];
           for(int i=0; i<sarray.length; i++){
               values[i] = Double.parseDouble(sarray[i]);
           }
           return Vectors.dense(values);
        });
        
        
        JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
        corpus.cache();
        OnlineLDAOptimizer olda = new OnlineLDAOptimizer();
        olda.setMiniBatchFraction(0.1);
        LDAModel ldaModel = new LDA().setOptimizer(olda).setK(topicCount).run(corpus);
//        System.out.println("Learned topics (as distribtions over vocab of " + ldaModel.vocabSize()
//      + " words):");
        long end = System.currentTimeMillis();
        System.out.println("Time without file operations: "+ (end-start));
        File file = new File(Document.ProjectConstants.LdaSrcDirectory+"/output/output"+outputFile);
        file.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        
        
        Tuple2<int[], double[]>[] to = ldaModel.describeTopics(termPerTopic);
        for(int i=0;i<to.length;i++){
            int[] term = to[i]._1();
            double[] wght = to[i]._2();
            for(int j=0;j<term.length;j++){
                bw.write(Document.LdaPreprocessor.invDictionary.get(term[j]) + " " + wght[j]+"\n");
            }
            bw.write("\n\n");
        }
        bw.close();
//        Matrix topics = ldaModel.topicsMatrix();
//        for (int topic = 0; topic < topicCount; topic++) {
//          ArrayList<TopicTerm> top = new ArrayList<>();
//          System.out.println("Topic " + topic + ":");
//          for (int word = 0; word < ldaModel.vocabSize(); word++) {
//            TopicTerm tt = new TopicTerm();
//            tt.word = Document.LdaPreprocessor.invDictionary.get(word);
//            tt.prob = topics.apply(word, topic);
//            top.add(tt);
//          }
//          Collections.sort(top,new comp());
//          for(int i=0;i<termPerTopic;i++){
//              System.out.println(top.get(i).word + " "+ top.get(i).prob);
//              bw.write(top.get(i).word + " "+ top.get(i).prob+"\n");
//          }
//          bw.write("\n\n");
//        }
//        bw.close();
        
    }
    public static void main(String args[]){
       
    }
    
}
