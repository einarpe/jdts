package kpp.jtds;

import kpp.jdts.csv.FileStringBuilder;
import kpp.jdts.importer.Importer;
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
      GlobalConfiguration.init(args[0]);
      FileStringBuilder.loadConfig();
      Importer.loadConfig();
      DTS.createFromXml().run();
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
