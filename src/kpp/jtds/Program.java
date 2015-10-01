package kpp.jtds;

import kpp.jtds.core.DTS;
import kpp.jtds.core.Logger;

public class Program
{

  public static void main(String[] args)
  {
    if (args.length == 0)
    {
      printHelp();
      return;
    }
    
    try
    {
      DTS.createFromXml(args[0]).run();
    }
    catch (Throwable ex)
    {
      Logger.error(ex.getMessage());
    }
  }

  private static void printHelp()
  {
    System.out.println("Usage:");
    System.out.println("java -jar jdts.jar config.xml");
  }

}
