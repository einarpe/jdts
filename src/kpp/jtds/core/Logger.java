package kpp.jtds.core;

import com.google.common.base.Joiner;

public final class Logger
{
  public static java.util.logging.Logger log = java.util.logging.Logger.getLogger("kpp.jdts");
  
  private static boolean isDebug = false;
  
  static
  {
    isDebug = System.getProperty("debug") != null ;
  }
  
  public static void info(Object ... msg)
  {
    log.info(join(msg));
  }
  
  public static void error(Object ... msg)
  {
    log.severe(join(msg));
  }
  
  private static String join(Object[] msg)
  {
    return Joiner.on("").join(msg);
  }

  public static void debug(String ... msg)
  {
    if (isDebug)
      log.info(join(msg));
  }

}
