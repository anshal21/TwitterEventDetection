/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;
import org.apache.derby.iapi.sql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class SparkLDA {

    /**
     * @param args the command line arguments
     */
    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;
    private int topicCount;
    private int termPerTopic;
    String outputFile;
    
    public SparkLDA(SparkSession sparkSession, int topicCount,int termPerTopic,String outputFile){
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
    public void fit(JavaRDD<Vector> parsedData) {
        try {
            System.out.println("===================================================================================");
            
            
            JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
            corpus.cache();
            OnlineLDAOptimizer olda = new OnlineLDAOptimizer();
            olda.setMiniBatchFraction(0.1);
            LDAModel ldaModel = new LDA().setOptimizer(olda).setK(topicCount).run(corpus);

            long end = System.currentTimeMillis();
            Configuration config = new Configuration();
            config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
            config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path("output"+outputFile);
            if(fs.exists(outputPath))
                fs.delete(outputPath);
            OutputStream os = fs.create(outputPath);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            String line;
    //        File file = new File(Document.ProjectConstants.LdaSrcDirectory+"/output/output"+outputFile);
    //        file.createNewFile();
    //        BufferedWriter bw = new BufferedWriter(new FileWriter(file));


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
        } catch (IOException ex) {
            Logger.getLogger(SparkLDA.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
   
    public static void main(String args[]){
       
    }
    
}
