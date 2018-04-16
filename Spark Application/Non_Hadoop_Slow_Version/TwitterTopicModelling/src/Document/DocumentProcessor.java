/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Document;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


/**
 *
 * @author anshal
 */
public class DocumentProcessor implements Serializable{
    
    public String sourceDirectory;
    public String destinationDirectory;
    public SparkSession sparkSession;
    public static Set<String> dictionary = new HashSet<String>();
    
    Set<String> stopWords;
    JavaRDD<Document> document;
    public final String STOP_WORDS = "/usr/local/spark/stopWords";
    //public final String STOP_WORDS = "/home/anshal/Desktop/np/stopWords";
    public JavaSparkContext javaSparkContext;
    
    public DocumentProcessor(){
        
    }
    
    public DocumentProcessor(String sourceDirectory, String destinationDirectory, SparkSession sparkSession){
        debug("Constructor Called");
        this.sourceDirectory = sourceDirectory;
        this.destinationDirectory = destinationDirectory;
        this.sparkSession = sparkSession;
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        
        DocumentProcessor dp = new DocumentProcessor();
        
        File[] files = new File(sourceDirectory).listFiles();
        
        debug(sourceDirectory);
        dp.loadStopWords();
        
        for(final File file : files){
                document = javaSparkContext.textFile(file.getAbsolutePath()).map((String text) -> {
                System.out.println("here");
                Document doc = new Document();
                doc.setName(file.getName());
                doc.setContent(text.toLowerCase());
                dp.processDocument(doc);
                System.out.println(doc.getContent());
                return doc;
            });
            document.collect().forEach(System.out::println);
        }
      
        
        
    }
    
    public final void loadStopWords() {
        try {
            stopWords = new HashSet<>();
            Scanner sc = new Scanner(new File(STOP_WORDS));
            while(sc.hasNext()){
                String word = sc.next().trim();
                stopWords.add(word);
            }
           
            System.out.println("Done loading stopwords...");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DocumentProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String readDocument(String fileName) throws FileNotFoundException{
        StringBuilder content = new StringBuilder();
        Scanner sc = new Scanner(new File(fileName));
        while(sc.hasNext()){
            content.append(sc.next());
        }
        
        return content.toString().trim();
    }
    
    public final void processDocument(Document doc){
        try {
            CharArraySet ch = new CharArraySet(Version.LUCENE_48, stopWords, true);
            TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_48, new StringReader(doc.getContent()));
            tokenStream = new StopFilter(Version.LUCENE_36, tokenStream,ch);
            tokenStream = new PorterStemFilter(tokenStream);
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            Set<String> uniqueWords = new HashSet<>();
            Map<String,Integer> wordFrequency = new HashMap<String,Integer>();
            tokenStream.reset();
            while(tokenStream.incrementToken()){
                String word = charTermAttribute.toString();
                uniqueWords.add(word);
                if(wordFrequency.containsKey(word))
                    wordFrequency.put(word,wordFrequency.get(word)+1);
                else
                    wordFrequency.put(word,1);
                dictionary.add(word);
                    
            }
            doc.setUniqueWords(uniqueWords);
            doc.setWordFrequency(wordFrequency);
           
           
        } catch (IOException ex) {
            Logger.getLogger(DocumentProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
       
    }
    public void debug(String debugStatement){
        System.out.println(debugStatement);
    }
    public static void main(String args[]){
        SparkSession spark = SparkSession.builder().appName("Temp").getOrCreate();
        spark.conf().set("mapred.max.split.size", "10000000");
        DocumentProcessor dp = new DocumentProcessor("/home/anshal/Desktop/Dont Open/topic_modeling/srcFiles/","", spark);
        
    }
}
