using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Collections.Specialized;

using System.Runtime.InteropServices;
using System.EnterpriseServices;

using ESRI.ArcGIS.esriSystem;
using ESRI.ArcGIS.Server;
using ESRI.ArcGIS.Geometry;
using ESRI.ArcGIS.Geodatabase;
using ESRI.ArcGIS.Carto;
using ESRI.ArcGIS.SOESupport;


//TODO: sign the project (project properties > signing tab > sign the assembly)
//      this is strongly suggested if the dll will be registered using regasm.exe <your>.dll /codebase


namespace eSpeciesSOE
{
    [ComVisible(true)]
    [Guid("a52acbb1-ea82-4660-83aa-b19248110cc4")]
    [ClassInterface(ClassInterfaceType.None)]
    public class eSpeciesSOE : ServicedComponent, IServerObjectExtension, IObjectConstruct, IRESTRequestHandler
    {
        private string soe_name;

        private IPropertySet configProps;
        private IServerObjectHelper serverObjectHelper;
        private ServerLogger logger;
        private IRESTRequestHandler reqHandler;

        public eSpeciesSOE()
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
        }

        #endregion

        #region IObjectConstruct Members

        public void Construct(IPropertySet props)
        {
            configProps = props;
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
            RestOperation sampleOper = new RestOperation("getSpeciesListForQuadkey", new string[] { "quadkey" }, new string[] { "json" }, getSpeciesListForQuadkeyHandler);
            RestOperation sampleOper2 = new RestOperation("getSpeciesListForBBox", new string[] { "txmin", "txmax", "tymin", "tymax" }, new string[] { "json" }, getSpeciesListForBBoxHandler);
            rootRes.operations.Add(sampleOper);
            rootRes.operations.Add(sampleOper2);
            return rootRes;
        }

        private byte[] RootResHandler(NameValueCollection boundVariables, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            JsonObject result = new JsonObject();
            result.AddString("This SOE provides operations to retrieve information on species","");
            return Encoding.UTF8.GetBytes(result.ToJson());
        }

        private byte[] getSpeciesListForQuadkeyHandler(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            string quadkey;
            double? quadkeydbl; //the quadkey is converted to a number by json so we must get it as a number - e.g. 132320330230023
            operationInput.TryGetAsDouble("quadkey", out quadkeydbl); //get the quadkey
            quadkey = quadkeydbl.ToString(); //convert to a string
            Type factoryType = Type.GetTypeFromProgID("esriDataSourcesGDB.FileGDBWorkspaceFactory"); //open a connection to the species data table
            IWorkspaceFactory workspaceFactory = (IWorkspaceFactory)Activator.CreateInstance(factoryType);
            IWorkspace workspace = workspaceFactory.OpenFromFile("D:\\GIS Data\\Andrew\\PilotSpeciesData.gdb", 0); //TODO make this more sustainable
            IFeatureWorkspace featureWorkspace = (IFeatureWorkspace)workspace;
            IQueryDef queryDef = featureWorkspace.CreateQueryDef(); //create a query to get the data
            queryDef.Tables = "PilotSpeciesData,Species"; //specify the tables
            queryDef.SubFields = "PilotSpeciesData.species_ID,Species.tax_id,Species.friendly_name"; //specify the fields that you will return
            queryDef.WhereClause = "PilotSpeciesData.quadkey='" + quadkey + "' and PilotSpeciesData.species_ID=Species.tax_id"; //create the query
            ICursor cursor = queryDef.Evaluate();
            int friendly_nameIndex = cursor.FindField("Species.friendly_name");
            IRow row = null;
            String s = "";
            while ((row = cursor.NextRow()) != null) //get the resultset and iterate through the records
            {
                s = s + row.get_Value(friendly_nameIndex) + ",";
            }
            JsonObject result = new JsonObject();
            result.AddString("species", s); //write the results
            return Encoding.UTF8.GetBytes(result.ToJson()); //return the results
        }
        private byte[] getSpeciesListForBBoxHandler(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            long? txmin;
            long? txmax;
            long? tymin;
            long? tymax;
            operationInput.TryGetAsLong("txmin", out txmin);
            operationInput.TryGetAsLong("txmax", out txmax);
            operationInput.TryGetAsLong("tymin", out tymin);
            operationInput.TryGetAsLong("tymax", out tymax);
            Type factoryType = Type.GetTypeFromProgID("esriDataSourcesGDB.FileGDBWorkspaceFactory"); //open a connection to the species data table
            IWorkspaceFactory workspaceFactory = (IWorkspaceFactory)Activator.CreateInstance(factoryType);
            IWorkspace workspace = workspaceFactory.OpenFromFile("D:\\GIS Data\\Andrew\\PilotSpeciesData.gdb", 0); //TODO make this more sustainable
            IFeatureWorkspace featureWorkspace = (IFeatureWorkspace)workspace;
            IQueryDef queryDef = featureWorkspace.CreateQueryDef(); //create a query to get the data
            IQueryDef2 queryDef2 = (IQueryDef2)queryDef;
            queryDef2.Tables = "Species"; //specify the tables
            queryDef2.SubFields = "Species.tax_id,Species.friendly_name"; //specify the fields that you will return
            queryDef2.WhereClause = "tax_id IN (SELECT species_ID from PilotSpeciesData where mx between  " + txmin.ToString() + " and " + txmax.ToString() + " and my between " + tymin.ToString() + " and " + tymax.ToString() + ")"; //create the query
            queryDef2.PrefixClause = "DISTINCT";
            ICursor cursor = queryDef2.Evaluate();
            int friendly_nameIndex = cursor.FindField("Species.friendly_name");
            IRow row = null;
            String s = "";
            while ((row = cursor.NextRow()) != null) //get the resultset and iterate through the records
            {
                s = s + row.get_Value(friendly_nameIndex) + ",";
            }
            JsonObject result = new JsonObject();
            result.AddString("species", s); //write the results
            return Encoding.UTF8.GetBytes(result.ToJson()); //return the results
        }


    }
}
