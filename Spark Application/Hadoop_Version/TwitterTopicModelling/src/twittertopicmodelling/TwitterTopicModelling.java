/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twittertopicmodelling;
import Document.LdaPreprocessor;
import Document.ProjectConstants;
import Model.SparkLDA;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import org.apache.spark.sql.Dataset;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        SparkSession spark = SparkSession.builder().appName("Temp").getOrCreate();
        SparkSession spark2 = SparkSession.builder().master("spark://hadoopMaster:7077").appName("Temp").getOrCreate();
        Configuration config = new Configuration();
        config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(config);
        Path inputPath = new Path(srcFileName);
        InputStream is = fs.open(inputPath);
        BufferedReader buffereReader = new BufferedReader(new InputStreamReader(is));
        String line;
        
        while((line=buffereReader.readLine())!=null){
            List<String> curr = new ArrayList<>();
            /*Path outputPath = new Path("Tweets"+cnt);
                        OutputStream os = fs.create(outputPath);
                        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os));
                        bufferedWriter.append(line);*/
            curr.add(line);
            for(int i=0;i<batchSize-1;i++){
                if((line=buffereReader.readLine())!=null)
                    curr.add(line);
                else
                    break;
            }
            long start = System.currentTimeMillis();
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<String> s = jsc.parallelize(curr);
            s.count();
            Model.SparkLDA sparkLda = new SparkLDA(spark2,numOfTopic,termsPerTopic,cnt+"");
            LdaPreprocessor ldaPreprocessor = new LdaPreprocessor(sparkLda,s, spark, cnt+"");
            long end = System.currentTimeMillis();
            System.out.println("Time : "+(-start+end));
            cnt++;
        }
        
       
        
        
        
//        Scanner sc = new Scanner(new File(Document.ProjectConstants.LdaSrcDirectory+srcFileName));
//
//        
//        while(sc.hasNext()){
//            if(Document.DocumentProcessor.dictionary!=null)
//                 Document.DocumentProcessor.dictionary.clear();
//            if(Document.LdaPreprocessor.invDictionary!=null)
//                 Document.LdaPreprocessor.invDictionary.clear();
//            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Document.ProjectConstants.LdaSrcDirectory+"Tweets"+cnt)));
//            for(int i=0; i<batchSize && sc.hasNext();i++){
//                bw.write(sc.nextLine()+"\n");
//            }
//            bw.close();
//            long start = System.currentTimeMillis();
//            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//            JavaRDD<String> s = jsc.textFile(ProjectConstants.documentSrcDirectory+"Tweets"+cnt);
//            Model.SparkLDA sparkLda = new SparkLDA(spark2,numOfTopic,termsPerTopic,cnt+"");
//            LdaPreprocessor ldaPreprocessor = new LdaPreprocessor(sparkLda,s, spark, cnt+"");
//            //System.out.println("Running LDA on batch"+cnt);
//            
////            
//            //  sparkLda.fit();
//            long end = System.currentTimeMillis();
//            System.out.println("Time : "+(-start+end));
//            
//            cnt++;  
//        }
        
    
        
        
    }
    
}
