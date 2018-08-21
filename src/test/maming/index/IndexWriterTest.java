package maming.index;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class IndexWriterTest {

	  private Document doc() {
		    Document doc = new Document();
		    for (int i = 0; i < 10; i++) {
		    	doc.add(field("name"+i,"value,value"+i));
		    }
		    return doc;
	  }
	  
	  private Field field(String name,String value) {
		  return new Field(name,value,StringField.TYPE_STORED);
	  }
	  
	  public Directory directory() throws IOException{
		  String path = "/Users/maming/Desktop/mm/document/test/lucene";
		  return new SimpleFSDirectory(new File(path));
	  }
	  
	  public IndexWriter indexWriter(Directory dir) throws IOException{
		  Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);  
		  IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_47,analyzer);
		  return new IndexWriter(dir, conf);
	  }
	public void test1(){
		try{
			Directory dir = directory();
			IndexWriter iw = indexWriter(dir);
		    int numDocs = 20;//创建20个document
		    for (int i = 0; i < numDocs; i++) {
		      iw.addDocument(doc());
		    }
		    iw.close();
		    dir.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	public static void main(String[] args) {
		IndexWriterTest test = new IndexWriterTest();
		test.test1();
		System.out.println("aa");
	}
}
