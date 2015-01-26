using System.ComponentModel;
using System.Transactions;
using System;
using System.Globalization;
using Neuron.Esb.Pipelines;
using System.IO.Ports;


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
    public partial class SerialAdapter
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
        private const string AdapterName = "Serial Adapter";
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

              

        private string _serialport = "COM1";
        private string _baudrate = "9600";

        [DisplayName("COMPort Name")]
        [Category("(General)")]
        [Description("The name of SerialPort to listening from.")]
        [DefaultValue("COM1")]
        [PropertyOrder(0)]
        public string COMPort
        {
            get { return _serialport; }
            set { _serialport = value; }
        }

        [DisplayName("BaudRate")]
        [Category("(General)")]
        [Description("The baud rate must be supported by the user's serial driver.")]
        [DefaultValue("9600")]
        [PropertyOrder(1)]
        public string BaudRate
        {
            get { return _baudrate; }
            set { _baudrate = value; }
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
        public SerialAdapter()
        {
            AdapterModes = new AdapterMode[]
            { 
                /// To control what is displayed and supporte in the neuron explorer UI, comment out the modes
                /// that you do not wish to support
               // new AdapterMode(AdapterModeStringsEnum.Subscriber.Description(), MessageDirection.DatagramSender),      // subscribe mode - send messages to an SerialPort
                new AdapterMode(AdapterModeStringsEnum.Publish.Description(), MessageDirection.DatagramReceiver),     // Receive mode - new files are obtained from SerialPort and published to the bus
            };

            ESBAdapterCapabilities caps = new ESBAdapterCapabilities();
            caps.AdapterName = AdapterName;
            caps.Prefix = MetadataPrefix;


            // SAMPLE: Sample context properties that will be exposed within Neuron. These can be viewed within the Set Properties Process Step 
            // **************************************************************
            caps.MetadataFieldInfo =
                MetadataPrefix + ".SerialPort:Name of SerialPort data is sent to or retreived from," +
                MetadataPrefix + ".BaudRate:bits per second,";
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
            if (System.Convert.ToInt32(BaudRate) < 0) throw new ArgumentOutOfRangeException("Baud Rate", "You must enter a valid BaudRate number (greater than zero)");
            if (string.IsNullOrEmpty(COMPort)) throw new ArgumentNullException("SerialPort", "You must enter the name of COM port to connect to");
            if (string.IsNullOrEmpty(BaudRate)) throw new ArgumentNullException("Baud Rate", "The BaudRate cannot be null.");


            // SAMPLE: Custom set metadata properties that will be provided with every message sent or received from Neuron
            // The IncludeMetadata flag is set by the "Include Metadata Properties" checkbox located on the General tab of an 
            // Adapter Endpoint within the Neuron ESB Explorer.
            // ***********************************
            if (IncludeMetadata)
            {
                MessageProperties.Add(new NameValuePair("SerialPort", COMPort));
                MessageProperties.Add(new NameValuePair("BaudRate", BaudRate));
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
                sp.Close();
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
        }
            
        

        /// <summary>
        /// Called by the TryReceive(), which is executed within a polling function within the framework's Adapter class.  
        /// This will be called on each poll interval defined by the PollingInterval property.
        /// All errors are handled pursuant to the error handling propeties defined by the "Error Reporting" and "Error on Polling" properties
        /// located in the framework's Adapter class.
        /// The core of the work is done by calling out to PublishMessageFromSource()
        /// </summary>
        private void ReceiveFromSource()
        {
            try
            {
                //*****************************************************************************
                PublishMessageFromSource();
            }
            finally
            {
                sp.Close();
            }
        }



        #endregion

       

        #region Publish Functions

        /// <summary>
        /// called from ReceiveFromSource(). 
        /// If an error is raised, the error is reported to the event log, and an attempt is made to audit the 
        /// message to the Neuron Audit database.
        /// Demonstrates creating an ESB Message from either Text or Bytes and then publishing that message to the bus
        /// </summary>

        string msg = String.Empty;
        System.IO.Ports.SerialPort sp = new System.IO.Ports.SerialPort();
               
                    
        private void PublishMessageFromSource()
        {
            ESBMessage message = null;
            try
            {
                try
                {
                    //PublishMessageFromSource - Removed if condition to check sp.IsOpen and performing a sp.Close followed by reinitialization of sp properties - this was repeating for every message received
                    //and was causing error: 
                    //Exception: Exception Type: System.InvalidOperationException
                    //Exception Message: 'PortName' cannot be set while the port is open.
                    //Moved sp reinitialization code into new condition to check, if sp is not intialized then setup all poperties and Open - one time only
                    if (!sp.IsOpen)
                    {
                        sp.PortName = COMPort;
                        sp.BaudRate = System.Convert.ToInt32(BaudRate);
                        sp.DiscardNull = true;
                        sp.StopBits = StopBits.One;
                        sp.Parity = Parity.None;
                        sp.Open();
                    }
                }
                catch (Exception ex)
                {
                    sp.Close();
                    message = CreateEsbMessage(ex.Message, MessageProperties, MetadataPrefix, PublishTopic);
                    PublishEsbMessage(message);
                }
                sp.DataReceived += serialport_DataReceived;
            }
            catch (Exception ex)
            {
                sp.Close();
                string msg = string.Format(CultureInfo.InvariantCulture, "The adapter endpoint encountered an error. {0}", ex.Message);
                String failureDetail = Helper.GetExceptionMessage(Configuration, msg, base.Name, AdapterName, base.PartyId, message, ex);

                // try to audit the message
                if (AuditOnFailurePublish && MessageAuditService != null) MessageAuditService.AuditMessage(message, ex, base.PartyId, failureDetail);
                throw new Exception(failureDetail);
            }
        }
        void serialport_DataReceived(object sender, SerialDataReceivedEventArgs e)
        {
            ESBMessage message = null;
            try
            {
                //serialport_DataReceived - Removed if NOT condition to check sp.IsOpen and performing a sp.Open
                //and was causing error: 
                //Exception: Exception Type: System.InvalidOperationException
                //Exception Message: The port is closed. or Access to the port 'COM2' is denied.
                //Additionally, the adapter was publising a lot of null or empty messages after the actual bytes of data were sent, causing a lot of empty messages in the bus.
                //Added condition to check if message is null or empty and then Publish.
                msg = sp.ReadExisting();
                if (msg != null && msg.Trim().Length != 0)
                {
                    message = CreateEsbMessage(msg, MessageProperties, MetadataPrefix, PublishTopic);
                    PublishEsbMessage(message);
                }
                
            }
            catch (Exception ex)
            {
                message = CreateEsbMessage(ex.Message, MessageProperties, MetadataPrefix, PublishTopic);
                PublishEsbMessage(message);
            }

        }
        #endregion 
    }
}
