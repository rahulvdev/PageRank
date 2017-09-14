

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Reads records that are delimited by a specifc begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {
  
  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";
  
  @Override
  public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
                                                         JobConf jobConf,
                                                         Reporter reporter) throws IOException {
    return new XmlRecordReader((FileSplit) inputSplit, jobConf);
  }
  
  /**
   * XMLRecordReader class to read through a given xml document to output xml
   * blocks as records as specified by the start tag and end tag
   * 
   */
  public static class XmlRecordReader implements
      RecordReader<LongWritable,Text> {
    private final byte[] beginTag;
    private final byte[] endTag;
    private final long begin;
    private final long end;
    private final FSDataInputStream fileInStream;
    private final DataOutputBuffer dataStore = new DataOutputBuffer();
    
    public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
    	beginTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
      endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
      
      // open the file and seek to the start of the split
      begin = split.getStart();
      end = begin + split.getLength();
      Path file = split.getPath();
      FileSystem fSys = file.getFileSystem(jobConf);
      fileInStream = fSys.open(split.getPath());
      fileInStream.seek(begin);
    }
    
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      if (fileInStream.getPos() < end) {
        if (readUntilMatch(beginTag, false)) {
          try {
        	  dataStore.write(beginTag);
            if (readUntilMatch(endTag, true)) {
              key.set(fileInStream.getPos());
              value.set(dataStore.getData(), 0, dataStore.getLength());
              return true;
            }
          } finally {
        	  dataStore.reset();
          }
        }
      }
      return false;
    }
    
    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }
    
    @Override
    public Text createValue() {
      return new Text();
    }
    
    @Override
    public long getPos() throws IOException {
      return fileInStream.getPos();
    }
    
    @Override
    public void close() throws IOException {
    	fileInStream.close();
    }
    
    @Override
    public float getProgress() throws IOException {
      return (fileInStream.getPos() - begin) / (float) (end - begin);
    }
    
    private boolean readUntilMatch(byte[] match, boolean insideBlk) throws IOException {
      int index = 0;
      while (true) {
        int readVal = fileInStream.read();
        // end of file:
        if (readVal == -1) return false;
        // save to buffer:
        if (insideBlk) dataStore.write(readVal);
        
        // check if we're matching:
        if (readVal == match[index]) {
        	index++;
          if (index >= match.length) return true;
        } else index = 0;
        // see if we've passed the stop point:
        if (!insideBlk && index == 0 && fileInStream.getPos() >= end) return false;
      }
    }
  }
}