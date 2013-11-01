package org.apache.blur.store.tools;

import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

/**
 */
public class WriteLocalReadHdfsPerfTest {

    public static void main(String[] args) throws Exception {

        File localDir = Files.createTempDirectory("index").toFile();
        FSDirectory localDirectory = FSDirectory.open(localDir);
        try {

            FieldType binaryFieldType = new FieldType();
            binaryFieldType.setIndexed(false);
            binaryFieldType.setTokenized(false);
            binaryFieldType.setStored(true);
            binaryFieldType.freeze();

            String[] names = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};

            WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_43);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            try (IndexWriter indexWriter = new IndexWriter(localDirectory, config)) {
                for (int i = 0; i < 1000000; i++) {
                    Document document = new Document();
                    document.add(new TextField("uuid", UUID.randomUUID().toString(), Field.Store.YES));
                    document.add(new IntField("int", i, Field.Store.YES));
                    document.add(new TextField("name", names[i % names.length], Field.Store.YES));
                    document.add(new Field("data", new byte[10240], binaryFieldType));
                    indexWriter.addDocument(document);

                    if (i % 10000 == 0) {
                        System.out.println(i);
                    }
                }
                indexWriter.commit();
                indexWriter.forceMerge(1);
            }

            System.out.println(localDir.getAbsolutePath());

            //copy lucene dir to hdfs
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost");
            FileSystem fileSystem = FileSystem.get(conf);
            Path localIndexPath = new Path("/localindex");
            fileSystem.mkdirs(localIndexPath);
            fileSystem.copyFromLocalFile(new Path(localDir.getAbsolutePath() + "/"), localIndexPath);
            Path hdfsIndexPath = fileSystem.listStatus(localIndexPath)[0].getPath();

            HdfsDirectory hdfsDirectory = new HdfsDirectory(conf, hdfsIndexPath);
            Directory directory = new BlockCacheDirectoryFactoryV2(new Configuration(), 1000000).newDirectory(
                    "index", "shard1", hdfsDirectory, null
            );

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(reader);

                long start = System.currentTimeMillis();
                Query query = new QueryParser(Version.LUCENE_43, "name", analyzer).parse("r");
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

            hdfsDirectory.close();
            directory.close();
        } finally {
            localDirectory.close();
            FileDeleteStrategy.FORCE.delete(localDir);
        }
    }
}
