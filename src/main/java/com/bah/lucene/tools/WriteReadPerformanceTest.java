package com.bah.lucene.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.bah.lucene.BlockCacheDirectoryFactoryV2;
import com.bah.lucene.hdfs.HdfsDirectory;

import java.util.UUID;

/**
 */
public class WriteReadPerformanceTest {

    public static void main(String[] args) throws Exception {
        HdfsDirectory hdfsDirectory = new HdfsDirectory(new Configuration(), new Path("hdfs://localhost/index"));
        Directory directory = new BlockCacheDirectoryFactoryV2(new Configuration(), 1000000).newDirectory(
                "index", "shard1", hdfsDirectory, null
        );

        String[] names = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};

        WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        try (IndexWriter indexWriter = new IndexWriter(hdfsDirectory, config)) {
            for (int i = 0; i < 1000000; i++) {
                Document document = new Document();
                document.add(new TextField("uuid", UUID.randomUUID().toString(), Field.Store.YES));
                document.add(new IntField("int", i, Field.Store.YES));
                document.add(new TextField("name", names[i % names.length], Field.Store.YES));
                indexWriter.addDocument(document);

                if (i % 1000 == 0) {
                    System.out.println(i);
                }
            }
            indexWriter.commit();
            indexWriter.forceMerge(1);
        }

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher indexSearcher = new IndexSearcher(reader);

            long start = System.currentTimeMillis();
            Query query = new QueryParser("name", analyzer).parse("r");
            TopDocs search = indexSearcher.search(query, 1000);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            System.out.println("Found " + scoreDocs.length + " num of documents from search in " +
                    (System.currentTimeMillis() - start) +
                    " ms. Total[" + search.totalHits + "]");

            start = System.currentTimeMillis();
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document doc = indexSearcher.doc(scoreDoc.doc);
                assert doc != null;
            }
            System.out.println("Took [" + (System.currentTimeMillis() - start) + "] ms to retrieve all docs");
        }

        directory.close();
    }
}
