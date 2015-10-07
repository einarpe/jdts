package kpp.jtds.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

import kpp.jtds.GlobalConfiguration;

/**
 * DTS class
 * Responsible for reading configuration from XML file and running it.
 */
public class DTS
{
  private ConnectionData source;
  
  private ConnectionData destination;
  
  private LinkedList<Step> steps;
  
  /** Forbidden zone. */
  private DTS() { }
  
  /**
   * Create instance of class which will be based on XML file.
   * @return DTS instance
   * @throws Exception - when something goes wrong
   */
  public static DTS createFromXml() throws Exception
  {
    DTS dts = new DTS();
    
    dts.source = GlobalConfiguration.getSourceConnection();
    dts.destination = GlobalConfiguration.getDestinationConnection();
    dts.setSteps(GlobalConfiguration.getSteps());
    
    return dts;
  }
  
  private void setSteps(LinkedList<Step> stepList)
  {
    steps = stepList;
    for (Step stp : steps)
    {
      stp.setDTS(this);
    }
  }

  /**
   * Let's go!
   * Run all steps and exec functions. 
   * @throws Exception - when some step throws exception
   */
  public void run() throws Exception
  {
    Logger.info("Staring JDTS at ", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    if (steps.size() == 0)
    {
      Logger.info("No steps found, nothing to do.");
    }
    else
    {
      long start = System.currentTimeMillis();
      
      for (Step stp : steps)
        stp.execute();
      
      long end = System.currentTimeMillis() - start;
      
      BigDecimal timeInSeconds = new BigDecimal(end / 1000.0).setScale(2, RoundingMode.HALF_UP);
      Logger.info("Done in ", timeInSeconds.toString(), " s");
    }
  }
  
  /** Get connection to source database */
  public Connection getSourceConnection() throws SQLException
  {
    return source.getConnection();
  }
  
  /** Get connection to destination database */
  public Connection getDestConnection() throws SQLException
  {
    return destination.getConnection();
  }
}
