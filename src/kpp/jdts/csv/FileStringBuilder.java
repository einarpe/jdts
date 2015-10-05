package kpp.jdts.csv;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import kpp.jtds.GlobalConfiguration;

/**
 * FileStringBuilder acts like StringBuilder, but it works with files.
 * It uses buffer for storing data. When that buffer exceeds defined length, all data from buffer is saved to file.
 */
public class FileStringBuilder implements Closeable
{
  /** Max buffer size (bytes) */
  private static int buffSize = 8 * 1024 * 1024;  // 8 MB as default
  
  /** Path to store temporary CSV files */
  private static String tempFilePath = null;
  
  /** Do keep files when jdts finishes its job? */
  private static boolean keepFiles = false;
  
  /** Buffer */
  private StringBuilder sb;
  
  /** Buffer writer to file */
  private FileWriter fw;
  
  /** Temporary file we are writing to */
  private File file;
  
  
  public static void loadConfig()
  {
    buffSize = GlobalConfiguration.getBufferSize();
    
    GlobalConfiguration.TempFileConfig tfc = GlobalConfiguration.getTempFileConfig();
    tempFilePath = tfc.Dir;
    keepFiles = tfc.KeepFiles;
  }
  
  /**
   * Creates instance of class. Also creates temporary file in temporary catalog.
   * @throws IOException - when creating file fails
   */
  public FileStringBuilder() throws IOException
  {
    sb = createEmptyStringBuilder();
    setFile();
  }

  public void setFile() throws IOException
  {
    if (tempFilePath != null && !tempFilePath.isEmpty())
      file = File.createTempFile("jdts", ".csv", new File(tempFilePath));
    else
      file = File.createTempFile("jdts", ".csv");
    
    if (!keepFiles)
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
    
    if (sb.length() >= buffSize)
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
  public synchronized void close()
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
