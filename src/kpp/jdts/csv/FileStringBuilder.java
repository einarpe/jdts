package kpp.jdts.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Zapis danych do pliku tymczasowego
 *
 */
public class FileStringBuilder 
{
  private StringBuilder sb;
  
  private int limit;
  
  private FileWriter fw;
  
  private File file;
  
  public FileStringBuilder() throws IOException
  {
    sb = new StringBuilder();
    limit = 8 * 1024 * 1024; // 8 MB
    setFile();
  }
  
  public void setFile() throws IOException
  {
    file = File.createTempFile("jdts", ".csv");
    file.deleteOnExit();
    fw = new FileWriter(file);
  }
  
  public File getFile()
  {
    return file;
  }
  
  public FileStringBuilder append(Object o) throws Exception
  {
    sb.append(o);
    
    if (sb.length() >= limit)
      flush();
    
    return this;
  }
  
  public void flush() throws Exception
  {
    fw.write(sb.toString());
    fw.flush();
    
    sb = createEmptyStringBuilder();
  }
  
  public void close() throws Exception
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
  
  private StringBuilder createEmptyStringBuilder()
  {
    return new StringBuilder(0);
  }
}
