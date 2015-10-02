package kpp.jtds.core;

import java.util.logging.Level;


public final class Logger
{
  public static java.util.logging.Logger log = java.util.logging.Logger.getLogger("kpp.jdts");
  
  static
  {
    log.setLevel(System.getProperty("debug") != null ? Level.FINEST : Level.INFO);
    /*
    log.addHandler(new Handler() {
      
      @Override
      public void publish(LogRecord record)
      {
        System.out.println(record.getMessage());
      }
      
      @Override
      public void flush()
      {
        System.out.flush();
      }
      
      @Override
      public void close() throws SecurityException
      {
        // do nothing
      }
    });*/
  }
  
  public static void info(String ... msg)
  {
    log.info(join(msg));
  }
  
  public static void error(String ... msg)
  {
    log.severe(join(msg));
  }
  
  private static String join(String[] msg)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < msg.length; i++)
      sb.append(msg[i]);
    
    return sb.toString();
  }

  public static void debug(String ... msg)
  {
    log.fine(join(msg));
  }

}
