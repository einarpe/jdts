package kpp.jtds.core;



public final class Logger
{
  public static java.util.logging.Logger log = java.util.logging.Logger.getLogger("kpp.jdts");
  
  private static boolean isDebug = false;
  
  static
  {
    isDebug = System.getProperty("debug") != null ;
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
    if (isDebug)
      log.info(join(msg));
  }

}
