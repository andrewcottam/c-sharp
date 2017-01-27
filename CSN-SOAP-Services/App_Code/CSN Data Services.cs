using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Web.Services;
using System.Xml;

/// <summary>
/// Summary description for Contact
/// </summary>
[WebService(Namespace = "http://dev.unep-wcmc.org/CSNWebServices")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]

public class CSN_Data_Services : WebService
{
    private DBConn dbConnection;
    public CSN_Data_Services() 
    {
        dbConnection = new DBConn();
    }
    private Boolean validSql(string sql)
    {
        if ((sql.IndexOf("delete") > 0) || (sql.IndexOf("drop") > 0) || (sql.IndexOf("delete") > 0))
        {
            return false;
        }
        else
        {
            return true;
        }
    }
    [WebMethod(Description = "Returns a set of lookup values for a filter based on the passed SQL statement")]
    public XmlDataDocument GetLookupValues(string sql)
    {
        try
        {
            if (validSql(sql))
            {
                dbConnection.OpenSqlConnection();
                SqlCommand sqlCmd = dbConnection.CreateSQLCommand(sql);
                XmlReader xmlReader = sqlCmd.ExecuteXmlReader();
                XmlDataDocument xml = new XmlDataDocument();
                xml.Load(xmlReader);
                dbConnection.CloseSqlConnection();
                return xml;
            }
            else
            {
                return null;
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Data Retrieval Error", ex);
        }
    }
    [WebMethod(Description = "Returns a set of matching Critical Sites that match the passed SQL statement")]
    public XmlDataDocument GetMatchingCSNs(string sql)
    {
        try
        {
            if (validSql(sql))
            {
                dbConnection.OpenSqlConnection();
                SqlCommand sqlCmd = dbConnection.CreateSQLCommand(sql);
                XmlReader xmlReader = sqlCmd.ExecuteXmlReader();
                XmlDataDocument xml = new XmlDataDocument();
                xml.Load(xmlReader);
                dbConnection.CloseSqlConnection();
                return xml;
            }
            else
            {
                return null;
            }
        }
        catch (Exception ex)
        {
            throw new Exception("Data Retrieval Error", ex);
        }
    }
    [WebMethod(Description = "Returns the list of CSNs used in the project in pages.")]
    public DataSet GetSites(int startIndex, int pageSize)
    {
        try
        {
            dbConnection.OpenSqlConnection();
            SqlCommand sqlCmd = dbConnection.CreateSQLCommand("SELECT SiteRecID as a,Site.Name as b,Country as c,IsPoly as d,IsIBA as e FROM SITE INNER JOIN COUNTRY ON SITE.Country = COUNTRY.Code WHERE SITE.IsCSN=1 ORDER BY COUNTRY.CountryID, b");
            DataSet dSet = new DataSet();
            //int count = dSet.Tables[0].Rows.Count;
            SqlDataAdapter sqlDBAdapter = new SqlDataAdapter(sqlCmd);
            sqlDBAdapter.Fill(dSet, startIndex, pageSize, "data");
            dbConnection.CloseSqlConnection();
            return dSet;
        }
        catch (Exception ex)
        {
            throw new Exception("Data Rerieval Error", ex);
        }
    }
    [WebMethod(Description = "Returns the list of CSNs according to the SiteRecID IN clauseused in the project in pages.")]
    public DataSet GetSitesInClause(string inClause)
    {
        try
        {
            dbConnection.OpenSqlConnection();
            SqlCommand sqlCmd = dbConnection.CreateSQLCommand("SELECT SiteRecID as a,Site.Name as b,Country as c,IsPoly as d,IsIBA as e FROM SITE INNER JOIN COUNTRY ON SITE.Country = COUNTRY.Code WHERE SiteRecID IN (" + inClause + ") AND SITE.IsCSN=1 ORDER BY COUNTRY.CountryID, b");
            DataSet dSet = new DataSet();
            SqlDataAdapter sqlDBAdapter = new SqlDataAdapter(sqlCmd);
            sqlDBAdapter.Fill(dSet, "data");
            dbConnection.CloseSqlConnection();
            return dSet;
        }
        catch (Exception ex)
        {
            throw new Exception("Data Rerieval Error", ex);
        }
    }
    [WebMethod(Description = "Returns the list of CSNs used in the project in pages according to the search text.")]
    public DataSet GetSitesSearch(string searchText, int startIndex, int pageSize)
    {
        try
        {
            dbConnection.OpenSqlConnection();
            SqlCommand sqlCmd = dbConnection.CreateSQLCommand("SELECT SiteRecID as a,Site.Name as b,Country as c,IsPoly as d,IsIBA as e FROM SITE INNER JOIN COUNTRY ON SITE.Country = COUNTRY.Code WHERE (SITE.Name like '%" + searchText + "%') AND SITE.IsCSN=1 ORDER BY COUNTRY.CountryID, b");
            DataSet dSet = new DataSet();
            SqlDataAdapter sqlDBAdapter = new SqlDataAdapter(sqlCmd);
            sqlDBAdapter.Fill(dSet, startIndex, pageSize, "data");
            dbConnection.CloseSqlConnection();
            return dSet;
        }
        catch (Exception ex)
        {
            throw new Exception("Data Rerieval Error", ex);
        }
    }
    [WebMethod(Description = "Returns the list of CSNs used in the project in pages according to the search text and the SiteRecID In clause.")]
    public DataSet GetSitesSearchInClause(string searchText,  string inClause)
    {
        try
        {
            dbConnection.OpenSqlConnection();
            SqlCommand sqlCmd = dbConnection.CreateSQLCommand("SELECT SiteRecID as a,Site.Name as b,Country as c,IsPoly as d,IsIBA as e FROM SITE INNER JOIN COUNTRY ON SITE.Country = COUNTRY.Code WHERE (SITE.Name like '%" + searchText + "%') AND SiteRecID IN (" + inClause + ")AND SITE.IsCSN=1 ORDER BY COUNTRY.CountryID, b");
            DataSet dSet = new DataSet();
            SqlDataAdapter sqlDBAdapter = new SqlDataAdapter(sqlCmd);
            sqlDBAdapter.Fill(dSet, "data");
            dbConnection.CloseSqlConnection();
            return dSet;
        }
        catch (Exception ex)
        {
            throw new Exception("Data Rerieval Error", ex);
        }
    }
}