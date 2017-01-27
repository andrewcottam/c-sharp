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


namespace IUCNRedListSOE
{
    [ComVisible(true)]
    [Guid("2bdfbcdc-4c53-4d6d-a6d6-d4c86250a4a9")]
    [ClassInterface(ClassInterfaceType.None)]
    public class IUCNRedListSOE : ServicedComponent, IServerObjectExtension, IObjectConstruct, IRESTRequestHandler
    {
        private string soe_name;

        private IPropertySet configProps;
        private IServerObjectHelper serverObjectHelper;
        private ServerLogger logger;
        private IRESTRequestHandler reqHandler;
        private IFeatureClass speciesFeatureClass;
        public IUCNRedListSOE()
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
            // Read the properties.

            try
            {
                logger.LogMessage(ServerLogger.msgType.infoDetailed, "Construct", 8000, "Constructing IUCNRedListSOE");
                // Get the feature layer to be queried.
                IMapServer3 mapServer = (IMapServer3)serverObjectHelper.ServerObject;
                string mapName = mapServer.DefaultMapName;
                IMapLayerInfo layerInfo;
                IMapLayerInfos layerInfos = mapServer.GetServerInfo(mapName).MapLayerInfos;
                // Find the index position of the map layer to query.
                int c = layerInfos.Count;
                int layerIndex = 0;
                for (int i = 0; i < c; i++)
                {
                    layerInfo = layerInfos.get_Element(i);
                    if (layerInfo.Name == "Species")
                    {
                        layerIndex = i;
                        break;
                    }
                }
                // Using IMapServerDataAccess to get the data, allows you to support MSD-based services.
                IMapServerDataAccess dataAccess = (IMapServerDataAccess)mapServer;
                // Get access to the source feature class.
                speciesFeatureClass = (IFeatureClass)dataAccess.GetDataSource(mapName, layerIndex);
                if (speciesFeatureClass == null)
                {
                    logger.LogMessage(ServerLogger.msgType.error, "Construct", 8000, "SOE custom error: Layer name not found.");
                    return;
                }
                else
                {
                    logger.LogMessage(ServerLogger.msgType.infoDetailed, "Construct", 8000, "Succesfully found Species Layer.");
                }
            }
            catch
            {
                logger.LogMessage(ServerLogger.msgType.error, "Construct", 8000, "SOE custom error: Could not get the feature layer.");
            }
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

            RestOperation sampleOper = new RestOperation("GetSpeciesExtent", new string[] { "ID_NO" }, new string[] { "json" }, GetSpeciesExtentHandler);
            RestOperation getLegendOper = new RestOperation("GetLegendClasses", new string[] { "ID_NO" }, new string[] { "json" }, GetLegendClasses);
            RestOperation getSourceOper = new RestOperation("GetSources", new string[] { "ID_NO" }, new string[] { "json" }, GetSource);

            rootRes.operations.Add(sampleOper);
            rootRes.operations.Add(getLegendOper);
            rootRes.operations.Add(getSourceOper);

            return rootRes;
        }

        private byte[] RootResHandler(NameValueCollection boundVariables, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;

            JsonObject result = new JsonObject();

            return Encoding.UTF8.GetBytes(result.ToJson());
        }

        private byte[] GetSpeciesExtentHandler(NameValueCollection boundVariables,
                                                  JsonObject operationInput,
                                                      string outputFormat,
                                                      string requestProperties,
                                                  out string responseProperties)
        {
            responseProperties = "{\"Content-Type\" : \"text/javascript\"}";
            long? idnoValue; //out parameter for the ID_NO as a long
            operationInput.TryGetAsLong("ID_NO", out idnoValue); //get the ID_NO parameter
            IQueryFilter queryFilter = new QueryFilterClass(); //instantiate a filter for the passed species
            queryFilter.WhereClause = "ID_NO='" + idnoValue + "' AND Legend<>''"; //set the where clause
            IFeatureCursor featureCursor = speciesFeatureClass.Search(queryFilter, false); //get the feature cursor to the matching features
            IFeature feature = null; //for iterating through the features
            IGeometryCollection pGeometryCollection = new GeometryBagClass() as IGeometryCollection; //instantiate a geometry bag to get the extent
            object obj = Type.Missing; //needed to add geometries to the geometry bag
            while ((feature = featureCursor.NextFeature()) != null) //iterate through the matching features and add the geometries to the geometry bag
            {
                pGeometryCollection.AddGeometry(feature.ShapeCopy, ref obj, ref obj); //add the geometry
            }
            JsonObject result = new JsonObject(); //create the return json object
            IEnvelope extent = (pGeometryCollection as IGeometry).Envelope; //get the extent of the geometry bag
            JsonObject jsonExtent = Conversion.ToJsonObject(extent); //convert the extent to json
            //TODO: Set the spatial reference for the extent in the SOE - at the moment it is set in the client code
            result.AddObject("extent", jsonExtent); //write the extent to the result object
            result.AddString("featureCount", pGeometryCollection.GeometryCount.ToString()); //get the number of features
            return Encoding.UTF8.GetBytes(result.ToJson()); //return the json
        }
        private byte[] GetLegendClasses(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null; //
            long? idnoValue; //out parameter for the ID_NO as a long
            operationInput.TryGetAsLong("ID_NO", out idnoValue); //get the ID_NO parameter
            IQueryFilter queryFilter = new QueryFilterClass(); //instantiate a filter for the passed species
            queryFilter.WhereClause = "ID_NO='" + idnoValue + "' AND Legend<>''"; //set the where clause
            IFeatureCursor featureCursor = speciesFeatureClass.Search(queryFilter, false); //get the feature cursor to the matching features
            IFeature feature = null; //for iterating through the features
            int index = speciesFeatureClass.Fields.FindField("Legend");
            List<string> legendClasses = new List<string>();
            List<JsonObject> jsonObjects = new List<JsonObject>();
            while ((feature = featureCursor.NextFeature()) != null) //iterate through the matching features and add the legend classes
            {
                string legendClass = feature.get_Value(index) as string;
                if (InList(legendClasses, legendClass) == false)
                {
                    JsonObject legendClassJson = new JsonObject();
                    legendClassJson.AddString("legendclass", legendClass);
                    legendClassJson.AddString("imageurl", GetImageName(legendClass));
                    jsonObjects.Add(legendClassJson);
                    legendClasses.Add(legendClass);
                }
            }
            JsonObject result = new JsonObject(); //create the return json object
            result.AddArray("legendclasses", jsonObjects.ToArray());
            return Encoding.UTF8.GetBytes(result.ToJson()); //return the json
        }
        private Boolean InList(List<string> list, string value)
        {
            foreach (string str in list)
            {
                if (str == value) // Will match once
                {
                    return true;
                }
            }
            return false;
        }
        private string GetImageName(string value)
        {
            switch (value)
            {
                case "Extinct":
                    return "extinct.png";
                case "Introduced":
                    return "introduced.png";
                case "Native (resident)":
                    return "native.png";
                case "Origin uncertain":
                    return "uncertain.png";
                case "Possibly Extinct":
                    return "possibly.png";
                case "Reintroduced":
                    return "reintroduced.png";
                case "Extant (breeding)":
                    return "extantB.png";
                case "Extant (non breeding)":
                    return "extantNB.png";
                case "Extant (resident)":
                    return "extantR.png";
                case "Probably Extant (breeding)":
                    return "probExtantB.png";
                case "Probably Extant (non breeding)":
                    return "probExtantNB.png";
                case "Probably Extant (resident)":
                    return "probExtantR.png";
                default:
                    return "native.png";
            }
        }
        private byte[] GetSource(NameValueCollection boundVariables, JsonObject operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null; //
            long? idnoValue; //out parameter for the ID_NO as a long
            operationInput.TryGetAsLong("ID_NO", out idnoValue); //get the ID_NO parameter
            IQueryFilter queryFilter = new QueryFilterClass(); //instantiate a filter for the passed species
            queryFilter.WhereClause = "ID_NO='" + idnoValue + "'"; //set the where clause
            IFeatureCursor featureCursor = speciesFeatureClass.Search(queryFilter, false); //get the feature cursor to the matching features
            IFeature feature = null; //for iterating through the features
            int index = speciesFeatureClass.Fields.FindField("CITATION");
            List<string> sources = new List<string>();
            List<JsonObject> jsonObjects = new List<JsonObject>();
            while ((feature = featureCursor.NextFeature()) != null) //iterate through the matching features 
            {
                string source = feature.get_Value(index) as string;
                if (InList(sources, source) == false)
                {
                    JsonObject sourceJson = new JsonObject();
                    sourceJson.AddString("Source", source);
                    jsonObjects.Add(sourceJson);
                    sources.Add(source);
                }
            }
            JsonObject result = new JsonObject(); //create the return json object
            result.AddArray("sources", jsonObjects.ToArray());
            return Encoding.UTF8.GetBytes(result.ToJson()); //return the json
        }

    }
}
