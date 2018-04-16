/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twittertopicmodelling;
import Document.LdaPreprocessor;
import Document.ProjectConstants;
import Model.SparkLDA;
import java.io.BufferedWriter;
import org.apache.spark.sql.Dataset;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 *
 * @author anshal
 */
public class TwitterTopicModelling {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {
        int numOfTopic = Integer.parseInt(args[0]);
        int termsPerTopic = Integer.parseInt(args[1]);
        String srcFileName = args[2];
        int batchSize = Integer.parseInt(args[3]);
        int cnt=0;
        Scanner sc = new Scanner(new File(Document.ProjectConstants.LdaSrcDirectory+srcFileName));

        SparkSession spark = SparkSession.builder().appName("Temp").getOrCreate();
        
        while(sc.hasNext()){
            if(Document.DocumentProcessor.dictionary!=null)
                 Document.DocumentProcessor.dictionary.clear();
            if(Document.LdaPreprocessor.invDictionary!=null)
                 Document.LdaPreprocessor.invDictionary.clear();
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Document.ProjectConstants.LdaSrcDirectory+"Tweets"+cnt)));
            for(int i=0; i<batchSize && sc.hasNext();i++){
                bw.write(sc.nextLine()+"\n");
            }
            bw.close();
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<String> s = jsc.textFile(ProjectConstants.documentSrcDirectory+"Tweets"+cnt);
            LdaPreprocessor ldaPreprocessor = new LdaPreprocessor(s, spark, cnt+"");
            Model.SparkLDA sparkLda = new SparkLDA(spark,numOfTopic,termsPerTopic,cnt+"");
            sparkLda.fit();
            cnt++;  
        }
        
        
    }
    
}
