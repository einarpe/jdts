package kpp.jdts.importer;

import kpp.jtds.core.Step;

public class PostgreSQLImporter extends Importer
{

  public PostgreSQLImporter(Step step)
  {
    super(step);
  }

  @Override
  public void insert() throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  protected Object obj2str(Object object, String columnType)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected String getLoadDataInfileQuery()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected String getTruncateQuery()
  {
    // TODO Auto-generated method stub
    return null;
  }

}
