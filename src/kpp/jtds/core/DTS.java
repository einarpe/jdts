package kpp.jtds.core;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DTS
{
  private ConnectionData source;
  
  private ConnectionData destination;
  
  private ArrayList<Step> steps = new ArrayList<>();
  
  private DTS() { }
  
  /**
   * Utworzenie instancji DTS na podstawie elementu XML
   * @param xmlFilePath - ścieżka do pliku XML
   * @return
   * @throws Exception
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
  
  /** Uruchomienie DTSa */
  public void run() throws Exception
  {
    System.out.println("Staring DTS at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    if (steps.size() == 0)
      System.out.println("No steps found.");
    
    long start = System.currentTimeMillis();
    for (Step stp : steps)
      stp.execute();
    
    System.out.println("Done in " + new BigDecimal((System.currentTimeMillis() - start) / 1000.0).setScale(2, RoundingMode.HALF_UP) + " s");
  }
  
  /** Połączenie do źródła */
  public Connection getSourceConnection() throws SQLException
  {
    return source.getConnection();
  }
  
  /** Połączenie do celu */
  public Connection getDestConnection() throws SQLException
  {
    return destination.getConnection();
  }
}
