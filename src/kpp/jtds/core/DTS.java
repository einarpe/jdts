package kpp.jtds.core;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * DTS class
 * Responsible for reading configuration from XML file and running it.
 */
public class DTS
{
  private ConnectionData source;
  
  private ConnectionData destination;
  
  private LinkedList<Step> steps = new LinkedList<>();
  
  /** Forbidden zone. */
  private DTS() { }
  
  /**
   * Create instance of class which will be based on XML file.
   * @param xmlFilePath - path to xml file with all DTS configuration.
   * @return DTS instance
   * @throws Exception - when something goes wrong
   */
  public static DTS createFromXml(String xmlFilePath) throws Exception
  {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new File(xmlFilePath));
    
    XPathFactory xp = XPathFactory.newInstance();
    Element connSrc = (Element)xp.newXPath().evaluate("/dts/connections/source", doc.getDocumentElement(), XPathConstants.NODE);
    Element connDst = (Element)xp.newXPath().evaluate("/dts/connections/destination", doc.getDocumentElement(), XPathConstants.NODE);
    NodeList steps = (NodeList)xp.newXPath().evaluate("/dts/steps/*", doc.getDocumentElement(), XPathConstants.NODESET);
    
    DTS resultDTS = new DTS();
    
    resultDTS.source = ConnectionData.fromXml(connSrc);
    resultDTS.destination = ConnectionData.fromXml(connDst);
    
    for (int i = 0, l = steps.getLength(); i < l; i++)
    {
      Element step = (Element) steps.item(i);
      Step stepObject = Step.create(step, resultDTS);
      resultDTS.steps.add(stepObject);
    }
    
    return resultDTS;
  }
  
  /**
   * Let's go!
   * Run all steps and exec functions. 
   * @throws Exception - when some step throws exception
   */
  public void run() throws Exception
  {
    Logger.info("Staring DTS at ", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    if (steps.size() == 0)
      Logger.info("No steps found.");
    
    long start = System.currentTimeMillis();
    for (Step stp : steps)
      stp.execute();
    
    Logger.info("Done in ", new BigDecimal((System.currentTimeMillis() - start) / 1000.0).setScale(2, RoundingMode.HALF_UP).toString(), " s");
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
