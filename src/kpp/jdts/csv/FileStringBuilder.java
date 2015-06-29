package kpp.jdts.csv;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * FileStringBuilder acts like StringBuilder, but it works with files.
 * It uses buffer for storing data. When that buffer exceeds defined length, all data from buffer is saved to file.
 */
public class FileStringBuilder implements Closeable
{
  /** Buffer */
  private StringBuilder sb;
  
  /** Max buffer size (bytes) */
  private int limit;
  
  /** Buffer writer to file */
  private FileWriter fw;
  
  /** Temporary file we are writing to */
  private File file;
  
  /**
   * Creates instance of class. Also creates temporary file in temporary catalog.
   * @throws IOException - when creating file fails
   */
  public FileStringBuilder() throws IOException
  {
    sb = createEmptyStringBuilder();
    
    limit = 8 * 1024 * 1024; // 8 MB as default
    
    String buffSize = System.getProperty("buffsize");
    if (buffSize != null && !buffSize.isEmpty())
      limit = Integer.parseInt(buffSize);
    
    setFile();
  }
  
  public void setFile() throws IOException
  {
    file = File.createTempFile("jdts", ".csv");
    file.deleteOnExit();
    fw = new FileWriter(file);
  }
  
  /**
   * Returns handle to a temporary file.
   */
  public File getFile()
  {
    return file;
  }
  
  /**
   * Appends data to buffer and when necessary, flushes all data from buffer to file.
   * @param o - object to append
   * @return this object
   * @throws Exception - when erros in flushing appended data occurs 
   */
  public FileStringBuilder append(Object o) throws Exception
  {
    sb.append(o);
    
    if (sb.length() >= limit)
      flush();
    
    return this;
  }
  
  /**
   * Flushes buffer to temporary file.
   * @throws Exception
   */
  public void flush() throws Exception
  {
    fw.write(sb.toString());
    fw.flush();
    
    sb = createEmptyStringBuilder();
  }
  
  /**
   * Flushes buffer to file when not empty, and closes temporary file.
   */
  public void close()
  {
    try
    {
      if (fw == null)
        return;
      
      if (sb.length() > 0)
      {
        flush();
        sb = createEmptyStringBuilder();
      }
      
      fw.close();
      fw = null;
    }
    catch (Exception ex)
    {
      System.err.println(ex.getMessage());
    }
  }
  
  private StringBuilder createEmptyStringBuilder()
  {
    return new StringBuilder(0);
  }
}
