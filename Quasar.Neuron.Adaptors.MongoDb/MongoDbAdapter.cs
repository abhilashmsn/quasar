using System.ComponentModel;
using System.Transactions;
using System;
using System.Globalization;
using Neuron.Esb.Pipelines;
using System.IO.Ports;
using MongoDB.Bson;
using MongoDB.Driver;


/// <summary>
/// Namespace for adapter assembly. 
/// </summary>
/// <remarks>
/// The default namespace for the adapter assembly is defined in the project property's application tab. This can be changed.
/// The name of the assembly, also defined on the application tab of the project property page is the concatonation of the 
/// namespace + the name of the adapter class.  The adapter class name, "NameAdapter", should be changed to reflect the nature of the adapter. 
/// For instance, if this adapter was to communicate with SalesForce, "NameAdapter" could be changed to 
/// "SalesForceAdapter". Hence, in project property's application tab the user would also change the assembly name so that the last
/// segment of the name would be identical to the class name adopted. The full assembly name would be namespace + "." + class name.
/// </remarks>
namespace Neuron.Esb.Adapters
{
    /// <summary>
    /// Generally the developer would change the name of the class to reflect the nature of the adapter. The class name in the 
    /// Adapter.cs file would also have to be changed to ensure that both class names are identical since they are both marked
    /// as partial classes.
    /// </summary>
    public partial class MongoDbAdapter
    {
        #region Constants and types
        /// <summary>
        /// These 2 constants are important.  They are used within the Neuron ESB Explorer at runtime and design time for 
        /// reporting and changing any properties that the custom adapter implementation may need. 
        /// 
        /// For example, _metadataOutPrefix constant, is the prefix used for querying or editing any custom properties that the 
        /// custom adapter may wish to allow users to modify or access at runtime through the context.Data.GetProperty() or 
        /// context.Data.SetProperty() methods. Also all properties "registered" using this prefix in the adapter class's constructor
        /// are displayed in the Set Property Process Step.
        /// 
        /// For instance, if this class used a connection string to access a resource, that connection string could be provided at 
        /// design time by defining it as a property of the class.  However, if the developer wanted to grant users the ability to 
        /// provide that property at runtime (as it may change for each message), the user could register the property in the class's 
        /// constructor and then with each incoming message, inspect the message for the presence of that property, extract the value 
        /// and use it at runtime to access the resource.
        /// 
        /// The _adapterName constant defines a "user friendly name" that is used to in the Adapter Registration screen within the 
        /// Neuron ESB Explorer. Its the name listed in the drop down adapter list when registrating an adapter. 
        /// </summary>
        private const string MetadataPrefix = "prefix";
        private const string AdapterName = "MongoDb Adapter";
        #endregion

        /// <summary>
        /// There are a number of sample propeties defined. All properties defined will be displayed at design time
        /// in a property grid within the Adapter Endpoint configuration screen in the Neuron ESB Explorer. 
        /// These samples demonstrate how to make properties visible/invisible (PropertyAttributesProvider attribute) 
        /// based on the state of other properties. These samples also demonstrate how to use type converters (TypeConverter attribute)
        /// , ordering (PropertyOrder attribute) of properties, categories (Category attribute), etc.
        /// 
        /// By default, all properties will also be displayed in the Bindings Dialog form for binding to Environmental Variables.
        /// If a property should not be "bindable", i.e. should not be used with Environmental Variables, add the attribute, 
        /// ,Bindable(false),  to the property.  The sample Password property has this applied.
        /// </summary>
        #region SAMPLE: Public Properties to Expose and Control in UI

              

        private string _database = System.String.Empty;
        private string _collection = System.String.Empty;
        private string _connectionstring = "mongodb://localhost";
        private MongoServer mongo;

        [DisplayName("ConnectionString")]
        [Category("(General)")]
        [Description("MongoDb server connection string")]
        [DefaultValue("mongodb://localhost")]
        [PropertyOrder(0)]
        public string ConnectionString
        {
            get { return _connectionstring; }
            set { _connectionstring = value; }
        }

        [DisplayName("Database Name")]
        [Category("(General)")]
        [Description("The name of database for storing messages.")]
        [DefaultValue("")]
        [PropertyOrder(1)]
        public string Database
        {
            get { return _database; }
            set { _database = value; }
        }

        [DisplayName("Collection Name")]
        [Category("(General)")]
        [Description("The name of the collection for storing messages")]
        [DefaultValue("")]
        [PropertyOrder(2)]
        public string Collection
        {
            get { return _collection; }
            set { _collection = value; }
        }

        private ErrorLevel _level = ErrorLevel.Warning;
        [DisplayName("Error Level")]
        [Category("(General)")]
        [Description("Default error level messages will be written to the log")]
        [PropertyOrder(3)]
        public ErrorLevel Level
        {
            get { return _level; }
            set { _level = value; }
        }

        
        #endregion

        /// <summary>
        /// Create an initialized instance of the adapter
        /// </summary>
        /// <remarks>
        /// AdapterModes and ESBAdapterCapabilities must always be initialized so that the ESB
        /// framework and Neuron ESB Explorer UI can correctly interact with the adapter. Modes that are supported in the UI must listed.
        /// meta data i.e. esb message custom propeties that will be supported must be added, each one preceeded by a period (.), followed by the
        /// name of the property (no spaces), a semicolon, followed by the description of the property i.e.
        ///     .[NameOfProperty].[Description of property]
        /// These properties will be displayed in the Set Properties Process Step as well.
        /// </remarks>
        public MongoDbAdapter()
        {
            AdapterModes = new AdapterMode[]
            { 
                /// To control what is displayed and supporte in the neuron explorer UI, comment out the modes
                /// that you do not wish to support
                new AdapterMode(AdapterModeStringsEnum.Subscriber.Description(), MessageDirection.DatagramSender),      // subscribe mode - send messages to an MongoDb
               
                
            };

            ESBAdapterCapabilities caps = new ESBAdapterCapabilities();
            caps.AdapterName = AdapterName;
            caps.Prefix = MetadataPrefix;


            // SAMPLE: Sample context properties that will be exposed within Neuron. These can be viewed within the Set Properties Process Step 
            // **************************************************************
            caps.MetadataFieldInfo =
                MetadataPrefix + ".Database:Name of database is sent to or retreived from," +
                MetadataPrefix + ".Collection:name of the collection for storing data,";
           // **************************************************************
            Capabilities = caps;

        }

        #region Base Class call outs for the Adapter Interfaces

        /// <summary>
        /// This is called by the base adapter class's Connect() method. This should contain all application specific validation
        /// logic and initialization of resources used by the custom adapter.
        /// if an exception is thrown, it will be caught by the runtime and the adapter will fail to start up. It will appear as 
        /// error in the Neuron ESB Event log and will be in a stopped state within Endpoint Health.
        /// </summary>
        private void ConnectAdapter()
        {
            // SAMPLE:  Validation logic for basic properties should be done here
            // ***********************************
            
            if (string.IsNullOrEmpty(Database)) throw new ArgumentNullException("Database", "You must enter the name of the database to connect to");
            if (string.IsNullOrEmpty(Collection)) throw new ArgumentNullException("Collection", "You must enter the name of the collection to store data to.");


            // SAMPLE: Custom set metadata properties that will be provided with every message sent or received from Neuron
            // The IncludeMetadata flag is set by the "Include Metadata Properties" checkbox located on the General tab of an 
            // Adapter Endpoint within the Neuron ESB Explorer.
            // ***********************************
            if (IncludeMetadata)
            {
                MessageProperties.Add(new NameValuePair("Database", Database));
                MessageProperties.Add(new NameValuePair("Collection", Collection));
            }

            try
            {
                MongoClient client = new MongoClient(ConnectionString);
                mongo = client.GetServer();
                mongo.Connect();
            }
            catch (Exception ex)
            {
                RaiseAdapterInfo(ErrorLevel.Error, ex.StackTrace);
            }
        }

        /// <summary>
        /// This is called by the base adapter class's Disconnect() method
        /// All resources that the custom adapter uses should be cleaned up here.
        /// </summary>
        private void DisconnectResources()
        {
            RaiseAdapterInfo(ErrorLevel.Info, "Disconnecting all resources");

            try
            {
                // *** place clean up work here

            }
            catch (Exception ex)
            {
                RaiseAdapterError(ErrorLevel.Error,ex.Message, ex);
            }
        }


        /// <summary>
        /// Called from the base class's SendToEndpoint().  This be called by the Neuron ESB runtime for Subscription mode adapters
        /// 
        /// </summary>
        /// <param name="message">ESB Message handed to adapter from runtime</param>
        /// <param name="tx">Deprecated Property - This is no longer used and IS ALWAYS NULL</param>
        private void SendToDataSource(ESBMessage message, CommittableTransaction tx)
        {
            RaiseAdapterInfo(ErrorLevel.Verbose, "Received Message. MessageId " + message.Header.MessageId + " Topic " + message.Header.Topic);

            try
            {
                // if mode is solicit/Response, return a reply to the bus
                switch (this._adapterMode.ModeName)
                {
                    case AdapterModeStringConstants.Subscribe:
                        SendToDataSource(message);
                        break;

                    default:
                        throw new InvalidOperationException(string.Format("Error in {0}, mode {1} not supported", AdapterName, this._adapterMode.ModeName));
                }

            }
            catch (Exception ex)
            {
                // throw the error to ensure the runtime enforces any adapter policies. Once an a policy executes
                throw ex;
            }
        }



        #region Send Functions

        /// <summary>
        /// called from SendToDataSource(). 
        /// This is a one way send to a back end system, protocol, transport, etc
        /// </summary>
        private void SendToDataSource(ESBMessage message)
        {
            if (message.Text != String.Empty)
            {
                try
                {
                    if (mongo.State != MongoServerState.Disconnected)
                    {
                        if (mongo.DatabaseExists(Database))
                        {
                            var db = mongo.GetDatabase(Database);
                            using (mongo.RequestStart(db))
                            {
                                if (db.CollectionExists(Collection))
                                {
                                    var collection = db.GetCollection(Collection);

                                    var id = ObjectId.GenerateNewId();
                                    BsonDocument msg = new BsonDocument()
                                    .Add("_id", id)
                                    .Add("message", message.Text)
                                    .Add("Timestamp", DateTime.UtcNow.ToString("ddMMyyhhmmss"));
                                    collection.Insert(msg);
                                }
                                else
                                {
                                    var result = db.CreateCollection(Collection);
                                    if (result.Ok)
                                    {
                                        var collection = db.GetCollection(Collection);
                                        var id = ObjectId.GenerateNewId();
                                        BsonDocument msg = new BsonDocument()
                                        .Add("_id", id)
                                        .Add("message", message.Text);
                                        collection.Insert(msg);
                                    }
                                    else
                                    {
                                        RaiseAdapterInfo(ErrorLevel.Error, "Collection failed to create. Reason :" + result.ErrorMessage);
                                    }
                                }

                            }
                        }
                        else
                        {
                            RaiseAdapterInfo(ErrorLevel.Error, Database + ": Database doesn't exist");
                        }
                    }
                    else
                    {
                        RaiseAdapterInfo(ErrorLevel.Error, "MongoDb Server is down");
                    }


                }
                catch (Exception ex)
                {
                    // using multiple threads so raise error and audit 
                    RaiseAdapterError(ErrorLevel.Error, string.Format("MongoDb adapter failed to log the message."), ex);

                    throw ex;
                }
            }
        }

        #endregion



        #endregion

       
    }
}
