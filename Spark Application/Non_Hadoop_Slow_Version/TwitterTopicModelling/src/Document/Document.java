/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Document;


import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author anshal
 */
public class Document implements Serializable{
    private String name;
    private String content;
    private Map<String, Integer> wordFrequency;
    private Set<String> uniqueWords;
    public String getName(){
        return name;
    }
    
   public void setName(String name){
       this.name = name;
   }
   
   public String getContent(){
       return content;
   }
   
   public void setContent(String content){
       this.content = content;
   }
   
   public Map<String, Integer> getWordFrequency(){
       return wordFrequency;
   }
   
   public void setWordFrequency(Map<String,Integer> wordFrequency){
       this.wordFrequency = wordFrequency;
   }
   
   public void setUniqueWords(Set<String> uniqueWords){
       this.uniqueWords = uniqueWords;
   }
   
   public Set<String> getUniqueWords(){
       return this.uniqueWords;
   }
}
