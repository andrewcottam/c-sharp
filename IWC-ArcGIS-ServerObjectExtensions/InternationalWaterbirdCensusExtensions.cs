using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Collections.Specialized;

using System.Runtime.InteropServices;
using System.EnterpriseServices;

using ESRI.ArcGIS.esriSystem;
using ESRI.ArcGIS.Server;
using ESRI.ArcGIS.Geometry;
using ESRI.ArcGIS.Geodatabase;
using ESRI.ArcGIS.Carto;
using ESRI.ArcGIS.SOESupport;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Web.Script;
using System.Web.Script.Serialization;

namespace InternationalWaterbirdCensusExtensions
{
    [ComVisible(true)]
    [Guid("973b0e52-22b2-4677-9e61-b224e4403204")]
    [ClassInterface(ClassInterfaceType.None)]
    public class InternationalWaterbirdCensusExtensions : ServicedComponent, IServerObjectExtension, IObjectConstruct, IRESTRequestHandler
    {
        private string soe_name;

        private IPropertySet configProps;
        private IServerObjectHelper serverObjectHelper;
        private ServerLogger logger;
        private IRESTRequestHandler reqHandler;
        private SqlConnection sqlConn;

        public InternationalWaterbirdCensusExtensions()
        {
            soe_name = this.GetType().Name;
            logger = new ServerLogger();
            reqHandler = new SoeRestImpl(soe_name, CreateRestSchema()) as IRESTRequestHandler;
        }

        #region IServerObjectExtension Members

        public void Init(IServerObjectHelper pSOH)
        {
            serverObjectHelper = pSOH;
        }

        public void Shutdown()
        {
            if (sqlConn.State == ConnectionState.Open) sqlConn.Close();
        }

        #endregion

        #region IObjectConstruct Members

        public void Construct(IPropertySet props)
        {
            configProps = props;
            String connString = "Data Source=WCMC-GIS-03\\SQL2008WEB;Initial Catalog='csn';User Id=sde;Password=conserveworld;";
            sqlConn = new SqlConnection(connString);
            sqlConn.Open();
        }

        #endregion

        #region IRESTRequestHandler Members

        public string GetSchema()
        {
            return reqHandler.GetSchema();
        }

        public byte[] HandleRESTRequest(string Capabilities, string resourceName, string operationName, string operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            return reqHandler.HandleRESTRequest(Capabilities, resourceName, operationName, operationInput, outputFormat, requestProperties, out responseProperties);
        }

        #endregion

        private RestResource CreateRestSchema()
        {
            RestResource rootRes = new RestResource(soe_name, false, RootResHandler);

            RestOperation ValidateDataOperation = new RestOperation("ValidateData", new string[] { "validationType", "data" }, new string[] { "json", "html" }, ValidateData);
            RestOperation GetWISitecodesOperation = new RestOperation("GetWISiteCodes", new string[] { "data" }, new string[] { "json", "html" }, GetWISiteCodes);
            RestOperation GetWISiteSynonymsOperation = new RestOperation("GetWISiteSynonyms", new string[] { "excludeWISiteCodes","data" }, new string[] { "json", "html" }, GetWISiteSynonyms);
            RestOperation PostDataOperation = new RestOperation("PostData", new string[] { "userID", "data" }, new string[] { "json", "html" }, PostData);
            RestOperation findNearFeatsOp = new RestOperation("findNearFeatures", new string[] { "location", "distance" }, new string[] { "json" }, FindNearFeatures);
            RestOperation GetNewSiteCodeOp = new RestOperation("GetNewSiteCode", new string[] { "CountryCode" }, new string[] { "json" }, GetNewSiteCode);
            RestOperation UpdateCheckedOp = new RestOperation("UpdateCheckedCount", new string[] { "id","checked" }, new string[] { "json" }, UpdateCheckedCount);
            RestOperation GetUserSitesOp = new RestOperation("GetUserSites", new string[] { "userID" }, new string[] { "json" }, GetUserSites);
            RestOperation GetUserSiteCodesOp = new RestOperation("GetUserSiteCodes", new string[] { "userID" }, new string[] { "json" }, GetUserSiteCodes);
            rootRes.operations.Add(ValidateDataOperation);
            rootRes.operations.Add(GetWISitecodesOperation);
            rootRes.operations.Add(GetWISiteSynonymsOperation);
            rootRes.operations.Add(PostDataOperation);
            rootRes.operations.Add(findNearFeatsOp);
            rootRes.operations.Add(GetNewSiteCodeOp);
            rootRes.operations.Add(GetUserSitesOp);
            rootRes.operations.Add(GetUserSiteCodesOp);
            rootRes.operations.Add(UpdateCheckedOp);
            return rootRes;
        }
        private byte[] RootResHandler(NameValueCollection boundVariables, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            JsonObject result = new JsonObject();
            result.AddString("Description", "The International Waterbird Census Server Object Extensions are utility REST services for managing and querying the IWC database.");
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private byte[] ValidateData(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            string validationType;
            bool found = operationInput.TryGetString("validationType", out validationType);
            if (!found || string.IsNullOrEmpty(validationType)) throw new ArgumentNullException("validationType");
            JsonObject data;
            found = operationInput.TryGetJsonObject("data", out data);
            if (!found || (data == null)) throw new ArgumentNullException("data");
            object[] records;
            data.TryGetArray("records", out records);
            string invalidIDs = "";
            if (records.Length > 0) invalidIDs = GetInvalidIDs(records, validationType);
            JsonObject result = new JsonObject();
            result.AddString("validationType", validationType);
            result.AddString("invalidIDs", invalidIDs);
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private string GetInvalidIDs(object[] records, string validationType)
        {
            DataTable dt = new DataTable();
            DataColumn col = new DataColumn("id", typeof(int));
            dt.Columns.Add(col);
            col = new DataColumn("value", typeof(string));
            dt.Columns.Add(col);
            DataRow row;
            long? id;
            string value;
            row = dt.NewRow();
            foreach (JsonObject obj in records)
            {
                row = dt.NewRow();
                obj.TryGetAsLong("i", out id);//the id parameter is just named 'i' in the json to keep it small
                row["id"] = id;
                obj.TryGetString("v", out value);//the value parameter is just named 'v' in the json to keep it small
                row["value"] = value;
                dt.Rows.Add(row);
            }
            string storedProcedureName = "IWC_Validate" + validationType; //e.g. IWC_ValidateSpeciesCode for species
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@data", dt);
            param.SqlDbType = SqlDbType.Structured;
            SqlDataReader reader = cmd.ExecuteReader();
            string invalidIDs = "";
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    invalidIDs = invalidIDs + reader.GetInt32(0).ToString() + ",";
                }
            }
            reader.Close();
            if (invalidIDs.Length == 0)
            {
                return invalidIDs;
            }
            else
            {
                return invalidIDs.Substring(0, invalidIDs.Length - 1);
            }
        }
        private byte[] PostData(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            string userID;
            bool found = operationInput.TryGetString("userID", out userID);
            if (!found || string.IsNullOrEmpty(userID)) throw new ArgumentNullException("userID");
            JsonObject data;
            found = operationInput.TryGetJsonObject("data", out data);
            if (!found || (data == null)) throw new ArgumentNullException("data");
            object[] records;
            data.TryGetArray("records", out records);
            int recordsPosted = -1;
            if (records.Length > 0) recordsPosted = PostRecords(records, userID);
            JsonObject result = new JsonObject();
            result.AddLong("recordsPosted", recordsPosted);
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private int PostRecords(object[] records, String userID)
        {
            DataTable dt = new DataTable();
            DataColumn col = new DataColumn("siteCode", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("date", typeof(DateTime));
            dt.Columns.Add(col);
            col = new DataColumn("speciesCode", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("count", typeof(int));
            dt.Columns.Add(col);
            col = new DataColumn("coverage", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("quality", typeof(int));
            dt.Columns.Add(col);
            col = new DataColumn("userID", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("entryDate", typeof(DateTime));
            dt.Columns.Add(col);
            col = new DataColumn("checked", typeof(int));
            dt.Columns.Add(col);
            col = new DataColumn("census", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("method", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("water", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("ice", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("tidal", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("weather", typeof(string));
            dt.Columns.Add(col);
            col = new DataColumn("disturbed", typeof(string));
            dt.Columns.Add(col);
            DataRow row;
            string siteCode;
            string dateString;
            string speciesCode;
            long? count;
            string coverage;
            long? quality;
            string census;
            string method;
            string water;
            string ice;
            string tidal;
            string weather;
            string disturbed;
            row = dt.NewRow();
            foreach (JsonObject obj in records)
            {
                row = dt.NewRow();
                obj.TryGetString("a", out siteCode);
                row["siteCode"] = siteCode;
                obj.TryGetString("b", out dateString);
                row["date"] = DateTime.ParseExact(dateString, "d", CultureInfo.InvariantCulture);//MM/DD/YYYY format
                obj.TryGetString("c", out speciesCode);
                row["speciesCode"] = speciesCode;
                obj.TryGetAsLong("d", out count);
                row["count"] = count;
                obj.TryGetString("e", out coverage);
                row["coverage"] = coverage;
                obj.TryGetAsLong("f", out quality);
                row["quality"] = quality;
                row["userID"] = userID;
                row["entryDate"] = DateTime.Now;
                row["checked"] = -1;
                obj.TryGetString("g", out census);
                row["census"] = census;
                obj.TryGetString("h", out method);
                row["method"] = method;
                obj.TryGetString("i", out water);
                row["water"] = water;
                obj.TryGetString("j", out ice);
                row["ice"] = ice;
                obj.TryGetString("k", out tidal);
                row["tidal"] = tidal;
                obj.TryGetString("l", out weather);
                row["weather"] = weather;
                obj.TryGetString("m", out disturbed);
                row["disturbed"] = disturbed;
                dt.Rows.Add(row);
            }
            string storedProcedureName = "IWC_Post_Data";
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@data", dt);
            param.SqlDbType = SqlDbType.Structured;
            int rows=-1;
            try
            {
                rows = cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
            }
            return rows;
        }
        private byte[] FindNearFeatures(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            int layerID = 0;
            JsonObject jsonPoint;
            if (!operationInput.TryGetJsonObject("location", out jsonPoint)) throw new ArgumentNullException("location");
            IPoint location = Conversion.ToGeometry(jsonPoint, esriGeometryType.esriGeometryPoint) as IPoint;
            if (location == null) throw new ArgumentException("FindNearFeatures: invalid location", "location");
            double? distance;
            if (!operationInput.TryGetAsDouble("distance", out distance) || !distance.HasValue) throw new ArgumentException("FindNearFeatures: invalid distance", "distance");
            byte[] result = FindNearFeatures(layerID, location, distance.Value);
            return result;
        }
        private byte[] FindNearFeatures(int layerID, IPoint location, double distance)
        {
            if (layerID < 0) throw new ArgumentOutOfRangeException("layerID");
            if (distance <= 0.0) throw new ArgumentOutOfRangeException("distance");
            IMapServer3 mapServer = serverObjectHelper.ServerObject as IMapServer3;
            if (mapServer == null) throw new Exception("Unable to access the map server.");
            IGeometry queryGeometry = ((ITopologicalOperator)location).Buffer(distance);
            ISpatialFilter filter = new SpatialFilterClass();
            filter.Geometry = queryGeometry;
            filter.SpatialRel = esriSpatialRelEnum.esriSpatialRelIntersects;
            IQueryResultOptions resultOptions = new QueryResultOptionsClass();
            resultOptions.Format = esriQueryResultFormat.esriQueryResultJsonAsMime;
            AutoTimer timer = new AutoTimer(); //starts the timer
            IMapTableDescription tableDesc = GetTableDesc(mapServer, layerID);
            logger.LogMessage(ServerLogger.msgType.infoDetailed, "FindNearFeatures", -1, timer.Elapsed, "Finding table description elapsed this much.");
            IQueryResult result = mapServer.QueryData(mapServer.DefaultMapName, tableDesc, filter, resultOptions);
            return result.MimeData;
        }
        private IMapTableDescription GetTableDesc(IMapServer3 mapServer, int layerID)
        {
            ILayerDescriptions layerDescs = mapServer.GetServerInfo(mapServer.DefaultMapName).DefaultMapDescription.LayerDescriptions;
            long c = layerDescs.Count;
            for (int i = 0; i < c; i++)
            {
                ILayerDescription3 layerDesc = (ILayerDescription3)layerDescs.get_Element(i);
                if (layerDesc.ID == layerID)
                {
                    layerDesc.LayerResultOptions = new LayerResultOptionsClass();
                    layerDesc.LayerResultOptions.GeometryResultOptions = new GeometryResultOptionsClass();
                    layerDesc.LayerResultOptions.GeometryResultOptions.DensifyGeometries = true;
                    return (IMapTableDescription)layerDesc;
                }
            }
            throw new ArgumentOutOfRangeException("layerID");
        }
        private byte[] GetNewSiteCode(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            string CountryCode;
            bool found = operationInput.TryGetString("CountryCode", out CountryCode);
            if (!found || string.IsNullOrEmpty(CountryCode)) throw new ArgumentNullException("CountryCode");
            string storedProcedureName = "_IWC_GetMaxSiteCode";
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@countryCode", CountryCode);
            SqlDataReader reader = cmd.ExecuteReader();
            string newCode = null;
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    newCode = reader.GetString(0);
                }
            }
            reader.Close();
            string newnum = null;
            if (newCode != null)
            {
                newnum = Convert.ToString((Convert.ToInt32(newCode.Substring(2)) + 1));
                char pad = '0';
                newnum = newnum.PadLeft(5, pad);
            }
            if (newCode != null) newCode = CountryCode + newnum;
            JsonObject result = new JsonObject();
            result.AddString("newCode", newCode);
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private byte[] UpdateCheckedCount(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            long? id;
            long? checkedVal;
            bool found = operationInput.TryGetAsLong("id", out id);
            if (!found) throw new ArgumentNullException("id");
            found = operationInput.TryGetAsLong("checked", out checkedVal);
            if (!found) throw new ArgumentNullException("checked");
            string storedProcedureName = "_IWC_UpdateChecked";
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param1 = cmd.Parameters.AddWithValue("@id", id);
            SqlParameter param2 = cmd.Parameters.AddWithValue("@checked", checkedVal);
            cmd.ExecuteNonQuery();
            JsonObject result = new JsonObject();
            result.AddString("results", "Done");
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private byte[] GetWISiteCodes(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            JsonObject data;
            bool found = operationInput.TryGetJsonObject("data", out data);
            if (!found || (data == null)) throw new ArgumentNullException("data");
            object[] records;
            data.TryGetArray("records", out records);
            JsonObject result=null;
            if (records.Length > 0) result = GetWICodes(records);
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private JsonObject GetWICodes(object[] records)
        {
            DataTable dt = new DataTable();
            DataColumn syn = new DataColumn("syn", typeof(string));
            dt.Columns.Add(syn);
            DataRow row;
            string synonym;
            row = dt.NewRow();
            foreach (JsonObject obj in records)
            {
                row = dt.NewRow();
                obj.TryGetString("s", out synonym);//the synonym parameter is just named 's' in the json to keep it small
                row["syn"] = synonym;
                dt.Rows.Add(row);
            }
            string storedProcedureName = "IWC_GetWISiteCodes"; 
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@data", dt);
            param.SqlDbType = SqlDbType.Structured;
            SqlDataReader reader = cmd.ExecuteReader();
            List<SiteSynonym> siteCodes = new List<SiteSynonym>();
            SiteSynonym[] siteCodesArray=null;
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    SiteSynonym siteObj=new SiteSynonym();
                    siteObj.s=reader.GetString(0);
                    siteObj.c=reader.GetString(1);
                    siteCodes.Add(siteObj);
                }
                siteCodesArray = siteCodes.ToArray();
            }
            reader.Close();
            JsonObject jsonObj = new JsonObject();
            jsonObj.AddArray("siteCodes", siteCodesArray);
            return jsonObj;
        }
        private byte[] GetWISiteSynonyms(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            JsonObject data;
            Boolean? excludeWISiteCodes;
            bool found = operationInput.TryGetJsonObject("data", out data);
            if (!found || (data == null)) throw new ArgumentNullException("data");
            found = operationInput.TryGetAsBoolean("excludeWISiteCodes", out excludeWISiteCodes);
            if (!found || (data == null)) throw new ArgumentNullException("excludeWISiteCodes");
            object[] records;
            data.TryGetArray("records", out records);
            JsonObject result = null;
            if (records.Length > 0) result = GetWISynonyms(records, excludeWISiteCodes);
            return Encoding.UTF8.GetBytes(result.ToJson());
        }
        private JsonObject GetWISynonyms(object[] records, Boolean? excludeWISiteCodes)
        {
            DataTable dt = new DataTable();
            DataColumn syn = new DataColumn("syn", typeof(string));
            dt.Columns.Add(syn);
            DataRow row;
            string code;
            row = dt.NewRow();
            foreach (JsonObject obj in records)
            {
                row = dt.NewRow();
                obj.TryGetString("s", out code);//the synonym parameter is just named 's' in the json to keep it small
                row["syn"] = code;
                dt.Rows.Add(row);
            }
            string storedProcedureName = null;
            if (excludeWISiteCodes == true)
            {
                storedProcedureName = "IWC_GetWISiteSynonymsExcludingWISiteCodes";
            }
            else
            {
                storedProcedureName = "IWC_GetWISiteSynonyms";
            }
            SqlCommand cmd = new SqlCommand(storedProcedureName, sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@data", dt);
            param.SqlDbType = SqlDbType.Structured;
            SqlDataReader reader = cmd.ExecuteReader();
            List<SiteSynonym> siteCodes = new List<SiteSynonym>();
            SiteSynonym[] siteCodesArray = null;
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    SiteSynonym siteObj = new SiteSynonym();
                    siteObj.s = reader.GetString(0);
                    siteObj.c = reader.GetString(1);
                    siteCodes.Add(siteObj);
                }
                siteCodesArray = siteCodes.ToArray();
            }
            reader.Close();
            JsonObject jsonObj = new JsonObject();
            jsonObj.AddArray("siteCodes", siteCodesArray);
            return jsonObj;
        }
        private byte[] GetUserSites(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            String userID;
            bool found = operationInput.TryGetString("userID", out userID);
            if (!found || (userID == null)) throw new ArgumentNullException("userID");
            byte[] result = null;
            result = GetUserSiteFeatures(userID);
            return result;
        }
        private Byte[] GetUserSiteFeatures(String userID)
        {
            SqlCommand cmd = new SqlCommand("_IWC_GetUserSites", sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@userID", userID);
            SqlDataReader reader = cmd.ExecuteReader();
            String siteCodes = null;
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    siteCodes = siteCodes + "'" + reader.GetString(0) + "',";
                }
                siteCodes = siteCodes.Substring(0, siteCodes.Length - 1);
            }
            reader.Close();
            IMapServer3 mapServer = serverObjectHelper.ServerObject as IMapServer3;
            if (mapServer == null) throw new Exception("Unable to access the map server.");
            IQueryFilter filter = new QueryFilterClass();
            filter.WhereClause = "WI_Sitecode IN (" + siteCodes + ")";
            IQueryResultOptions resultOptions = new QueryResultOptionsClass();
            resultOptions.Format = esriQueryResultFormat.esriQueryResultJsonAsMime;
            AutoTimer timer = new AutoTimer(); //starts the timer
            IMapTableDescription tableDesc = GetTableDesc(mapServer, 0);
            logger.LogMessage(ServerLogger.msgType.infoDetailed, "GetUserSiteFeatures", -1, timer.Elapsed, "Finding table description elapsed this much.");
            IQueryResult result = mapServer.QueryData(mapServer.DefaultMapName, tableDesc, filter, resultOptions);
            return result.MimeData;
        }
        private Byte[] GetUserSiteCodes(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            String userID;
            bool found = operationInput.TryGetString("userID", out userID);
            if (!found || (userID == null)) throw new ArgumentNullException("userID");
            SqlCommand cmd = new SqlCommand("_IWC_GetUserSites", sqlConn);
            cmd.CommandType = CommandType.StoredProcedure;
            SqlParameter param = cmd.Parameters.AddWithValue("@userID", userID);
            SqlDataReader reader = cmd.ExecuteReader();
            String siteCodes = null;
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    siteCodes = siteCodes + "'" + reader.GetString(0) + "',";
                }
                siteCodes = siteCodes.Substring(0, siteCodes.Length - 1);
            }
            reader.Close();
            JsonObject jObject = new JsonObject();
            jObject.AddString("siteCodes", siteCodes);
            return Encoding.UTF8.GetBytes(jObject.ToJson());
        }
    }
}
