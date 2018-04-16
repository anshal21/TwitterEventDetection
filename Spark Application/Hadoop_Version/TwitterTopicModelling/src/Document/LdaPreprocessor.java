/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Document;

import Model.SparkLDA;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
/**
 *
 * @author anshal
 */
public class LdaPreprocessor implements Serializable{
    
    private int rows;
    String outputFile;
    private int columns;
    public static Map<Integer,String> invDictionary;
    public JavaRDD<Vector> corpus;
    private DocumentProcessor documentProcessor;
    private List<Document> ldaDocuments;
    
    int[][] matrix;
    
    private SparkSession sparkSession;
    
    public int getRows(){
        return rows;
    }
    
    public void setRows(int rows){
        this.rows = rows;
    }
    
    public int getColumns(){
        return columns;
    }
    
    public void setColumns(int columns){
        this.columns = columns;
    }
    public Model.SparkLDA LDA;
    public LdaPreprocessor(Model.SparkLDA LDA, JavaRDD<String> s, SparkSession sparkSession,String outputFile) throws IOException{
        
        this.sparkSession = sparkSession;
        this.outputFile = outputFile;
        documentProcessor = new DocumentProcessor();
        JavaRDD<Document> document = null;
        documentProcessor.loadStopWords();
        ldaDocuments = new ArrayList<>();
        
        
       
        
        
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        
        
          
          document = s.map((String text) -> {
                Document doc = new Document();
                doc.setContent(text.toLowerCase());
                documentProcessor.processDocument(doc);
                
                return doc;
           });
          document.count();
          
          setColumns(DocumentProcessor.dictionary.size()+10);
           corpus = document.map(doc -> {
               int[] p = generateRow(doc);
               double[] pp = new double[p.length];
               for(int i=0;i<p.length;i++)
                   pp[i] = (double)p[i];
               return Vectors.dense(pp);
           });
           buildInvDict();
           
           LDA.fit(corpus);

    }
    
    public void buildInvDict(){
        Iterator iterator = DocumentProcessor.dictionary.iterator();
        int cnt=0;
        invDictionary = new HashMap<>();
        while(iterator.hasNext()){
            String word = (String)iterator.next();
            invDictionary. put(cnt++, word); 
        }
    }
    
    public void buildFrequencyMatrix(){
        matrix = new int[getRows()][getColumns()];
        int rowIndex = 0;
        Iterator iterator = ldaDocuments.iterator();
        
        while(iterator.hasNext()){
            Document doc = (Document)iterator.next();
            matrix[rowIndex++] = generateRow(doc);
        }
        
    }
    
    public int[] generateRow(Document doc){
        Iterator iterator = DocumentProcessor.dictionary.iterator();
        int[] row = new int[getColumns()];
        int colCount = 0;
        
        while(iterator.hasNext()){
//            debug(colCount);
//            debug(getColumns());
            String word = (String)iterator.next();
            if(doc.getUniqueWords().contains(word) && doc.getWordFrequency().containsKey(word)){
                row[colCount] = doc.getWordFrequency().get(word);
            }
            else{
                row[colCount] = 0;  
            }
            
            colCount++;
        }
        return row;
    }
    
    void printMatrix(int[][] matrix) throws IOException{

        try {
            File file = new File(ProjectConstants.LdaSrcDirectory+"/input/"+outputFile+"input.LDA");
            if(file.exists())
                file.delete();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
            for(int i=0;i<getRows();i++){
                String row = "";
                for(int j=0;j<getColumns();j++){
                    row = row + matrix[i][j]+" ";
                }
                row += "\n";
               // System.out.println(row);
                bufferedWriter.append(row);
            }
            bufferedWriter.close();
            file = new File(ProjectConstants.LdaSrcDirectory+"/input/"+outputFile+"dictionary.LDA");
            if(file.exists())
                file.delete();
            bufferedWriter = new BufferedWriter(new FileWriter(file));
            Iterator it = DocumentProcessor.dictionary.iterator();
            while(it.hasNext()){
                String word = (String)it.next();
                bufferedWriter.write(word+"\n");
            }
            bufferedWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LdaPreprocessor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(LdaPreprocessor.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            
            
        }
        
       
    }
    
    public void debug(Object obj){
        System.out.println(obj);
    }
    
    public static void main(String args[]) throws IOException{
//         int  numOfTopic = Integer.parseInt(args[0]);
//         int  termsPerTopic = Integer.parseInt(args[1]);
//         String outputFile = args[2];
//         SparkSession spark = SparkSession.builder().appName("Temp").getOrCreate();
//         JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//         JavaRDD<String> s = jsc.textFile(outputFile);
//         LdaPreprocessor ldaPreprocessor = new LdaPreprocessor(s, spark, outputFile);
//         Model.SparkLDA sparkLda = new SparkLDA(spark,numOfTopic,termsPerTopic,outputFile);
//         sparkLda.fit();
    }
    
}

