using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

/// <summary>
/// Helper class to provide access to database
/// </summary>
public class DBConn
{
    private string connString;
    public SqlConnection sqlConn;

    public DBConn()
    {
        try
        {
            connString = ConfigurationManager.ConnectionStrings["CSN"].ConnectionString;
        }
        catch (Exception ex)
        {
            throw new ConfigurationErrorsException(
                "Required database isn't configured. Please add connection info to Web.config for 'WOW' database.", ex);
        }
        sqlConn = new SqlConnection(connString);
    }

    /// <summary>
    /// Open Connection for SQL transactions
    /// </summary>
    public void OpenSqlConnection()
    {
        if (sqlConn.State == ConnectionState.Closed)
            sqlConn.Open();
    }

    /// <summary>
    /// Close connection after transactions
    /// </summary>
    public void CloseSqlConnection()
    {
        if (sqlConn.State == ConnectionState.Open)
            sqlConn.Close();
    }

    /// <summary>
    /// Creates new SqlCommand for given query string
    /// </summary>
    /// <param name="queryString">Query String</param>
    /// <returns>SqlCommand</returns>
    public SqlCommand CreateSQLCommand(string queryString)
    {
        return new SqlCommand(queryString, sqlConn);
    }

    /// <summary>
    /// Creates and adds parameter to SqlCommand
    /// </summary>
    /// <param name="command">SqlCommand</param>
    /// <param name="parameterName">Paramater Name</param>
    /// <param name="value">Paramater Value</param>
    public void CreateParameter(SqlCommand command, string parameterName, object value)
    {
        SqlParameter param = command.CreateParameter();
        param.ParameterName = parameterName;
        param.Value = value;
        command.Parameters.Add(param);
    }

    /// <summary>
    /// Creates a new dataset and fills it with data for given query
    /// </summary>
    /// <param name="selectCmd">Select Query</param>
    /// <param name="tabName">TableName</param>
    /// <returns>DataSet</returns>
    public DataSet ReturnDataSet(string selectCmd, string tabName)
    {
        sqlConn.Open();
        DataSet dSet = new DataSet();
        SqlDataAdapter sqlDBAdapter = new SqlDataAdapter(selectCmd, sqlConn);
        sqlDBAdapter.Fill(dSet, tabName);
        sqlConn.Close();
        return dSet;
    }
}