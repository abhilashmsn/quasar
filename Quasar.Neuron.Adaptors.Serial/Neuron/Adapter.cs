using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Threading;
using System.Transactions;
using System.Globalization;
using Neuron.Esb.Administration;

namespace Neuron.Esb.Adapters
{
    /// <summary>
    /// This is part of the Custom adapter class. Generally the developer would only edit the Class name.
    /// everything else within this partial class are generic routines used to support the adapter
    /// base class.
    /// </summary>
    [TypeConverter(typeof(PropertiesTypeConverter))]
    public partial class SerialAdapter : ESBAdapterBase
    {

        #region Adapter Mode Enumeration and Classes
        /// <summary>
        /// Contains all the string constants for the modes the adapter supports so we
        /// can always have known spellings. These will be displayed in the Mode dropdown control located
        /// on the General Tab of an Adapter endpoint.  these are referenced in the
        /// AdapterModeStringsEnum enumeration. Delete the ones that are not going to be supported
        /// within the custom adapter
        /// </summary>
        public static class AdapterModeStringConstants
        {
            public const string Publish = "Publish";
            public const string Subscribe = "Subscribe";
            public const string SolicitResponse = "Solicit Response";
            public const string RequestReply = "RequestReply";
        }
        /// <summary>
        /// Contains all of the modes that the adapter supports. These are referenced in the 
        /// Adapter constructor method
        /// </summary>
        public enum AdapterModeStringsEnum
        {
            [Description(AdapterModeStringConstants.Publish)]
            Publish = 0,
            [Description(AdapterModeStringConstants.Subscribe)]
            Subscriber = 1,
            [Description(AdapterModeStringConstants.SolicitResponse)]
            SolicitResponse= 2,
            [Description(AdapterModeStringConstants.RequestReply)]
            RequestReply = 3
        }
        #endregion

        #region Constants and types
        /// <summary>
        /// This is used for the receive thread that is started to support polling operations (i.e. Publish mode)
        /// </summary>
        private const int ThirtySeconds = 90 * 1000;                            // thread timeout for join
        #endregion

        #region private properties
        private MessageAudit MessageAuditService { get; set; }
        private ESBConfiguration Configuration { get; set; }
        private List<NameValuePair> MessageProperties = new List<NameValuePair>();    // reference to the Neuron ESB custom message properties populated by custom adapter
        private Thread _receiveThread;                                          // Receive thread used to start polling function to support default Pubish mode
        private int _pollFailCount = 0;                                         // Used to track errors generated to support failure mode error handling options during polling
        private bool _reportPollingErrors = true;                               // Determines whether or not to report the error during polling. used in conjunction with failure mode error handling options
        private const int _maxPollFails = 1;                                    // defines maxmimum number of failures during polling before suppress consecutive errors option is executed
        public AdapterMode _adapterMode;                                        // Set in the Connect() method. The adapter mode selected at design time on the general tab of the adapter endpoint
        private ManualResetEvent _quitEvent = new ManualResetEvent(false);      // Used in polling mode to pause polling per PollingInterval property value
        #endregion

        /// <summary>
        /// Default properties displayed in design time property grid of the adapter. The first property 'PublishTopic' 
        /// controls what topic the message is publish to when the adapter is in Publish/Request-reply mode.  The 
        /// remaining properties control before for Polling and how to handle errors when Polling.  if the custom 
        /// adapter does not support polling and/or publish modes, these properties can be removed.
        /// </summary>
        #region Public Properties UI
        [DisplayName("Publish Topic")]
        [Category("Publish Mode Properties")]
        [Description("The Neuron topic that messages will be published to. Required for Publish mode.")]
        [TypeConverter(typeof(AdapterTopicListConverter))]
        [PropertyOrder(0)]
        public string PublishTopic { get; set; }


        private int _pollingInterval = 10;                                 // seconds
        [DisplayName("Polling Interval")]
        [Category("Publish Mode Properties")]
        [Description("The Interval between polling executions (seconds).")]
        [DefaultValue(10)]
        [PropertyOrder(1)]
        public int PollingInterval
        {
            get
            {
                return this._pollingInterval;
            }
            set
            {
                this._pollingInterval = Math.Max(1, value);
            }
        }
               
        private bool _auditOnFailurePublish = false;
        [Category("Publish Mode Properties")]
        [Description("Register failed message and exception with Neuron Audit database. Database must be enabled.")]
        [DisplayName("Audit Mesage On Failure")]
        [PropertyOrder(2)]
        [DefaultValue(false)]
        public bool AuditOnFailurePublish
        {
            get { return _auditOnFailurePublish; }
            set { _auditOnFailurePublish = value; }
        }

        [Category("Publish Mode Properties")]
        [Description("Determines how all errors are reported in Event Log and Neuron Logs. Either as Errors, Warnings or Informationals.")]
        [DisplayName("Error Reporting")]
        [PropertyOrder(3)]
        [TypeConverter(typeof(AdapterPublishErrorModesEnumConverter))]
        [DefaultValue(AdapterPublishErrorModesEnum.Warning)]
        public AdapterPublishErrorModesEnum ErrorMode { get; set; }

        [Category("Publish Mode Properties")]
        [Description("Determines if polling of data source continues on error and if consecutive errors are reported.")]
        [DisplayName("Error On Polling")]
        [PropertyOrder(4)]
        [TypeConverter(typeof(AdapterPublishErrorHandlingEnumConverter))]
        [DefaultValue(AdapterPublishErrorHandlingEnum.SuppressConsecutiveErrors)]
        public AdapterPublishErrorHandlingEnum ErrorHandling { get; set; }

       

        #endregion

        #region Base class overrides
        /// <summary>
        /// This routine is fired by the ESB framework when the adapter is started up
        /// </summary>
        /// <param name="adapterMode">Contains the adapter mode being requested</param>
        /// <remarks>
        /// ESB framework will have already set the properties.  The requested
        /// adapter mode is checked against supported modes for validity.
        public override void Connect(string adapterMode)
        {
            this._adapterMode = null;

            foreach (AdapterMode mode in AdapterModes)
            {
                // don't worry about capitalization
                if (adapterMode.Equals(mode.ModeName,StringComparison.InvariantCultureIgnoreCase))
                {
                    this._adapterMode = mode;
                    break;
                }
            }

            // IncludeMetadata is a option located in the General Tab of the Adapter endpoint. Its meant to control whether or not
            // custom message properties should be collected and persisted and/or retreived from the message
            if (IncludeMetadata) MessageProperties.Add(new NameValuePair("Mode", this._adapterMode.ModeName));

            // get a copy of the esb configuration.  Used primarily for custom error handling and for instantiating the Neuron Message 
            // Audit service used for publish mode exceptions.
            Configuration = this.Configuration;
            RaiseAdapterInfo(ErrorLevel.Verbose, Configuration == null ? "ESB Configuration Is NULL" : "ESB Configuration returned");

            // connect to Neuron audit service
            ConnectAuditService();

            // call user defined method
            ConnectAdapter();

            // start up the receive thread for publish/polling mode if supported
            if (this._adapterMode.ModeName.Equals(AdapterModeStringConstants.Publish))
            {
                this._receiveThread = new Thread(new ThreadStart(ReceiveThreadRoutine));
                // always use a background thread so we don't need to clean them up
                this._receiveThread.IsBackground = true;
                this._receiveThread.Start();
            }

           
        }

        public override void SendToEndpoint(ESBMessage message, CommittableTransaction tx)
        {
            // call user defined method subscribing to message received from the bus and sending out to some system
            SendToDataSource(message, tx);
        }
        /// <summary>
        /// This routine if fired by the ESB framework when the adapter is shutdown
        /// </summary>
        public override void Disconnect()
        {
            try
            {
                // call user defined method
                DisconnectResources();

              
            }
            catch(Exception ex)
            {
                string msg = string.Format("An error occurred releasing resources during the shutdown of the adapter,'{0}'. Exception: {1}", AdapterName,ex.ToString());
                RaiseAdapterInfo(ErrorLevel.Warning, msg);
            }

            try
            {
                if (Configuration != null) Configuration = null;

                if (MessageAuditService != null) MessageAuditService.Disconnect();
                MessageAuditService = null;

                this._quitEvent.Set();

                // wait for thread to shutdown nicely, otherwise abort it so it isn't orphaned.  If you
                // are using background threads this should not be an issue, but it can happen.
                if (this._receiveThread != null && !this._receiveThread.Join(ThirtySeconds))
                    this._receiveThread.Abort();
            }
            catch(ThreadAbortException) // we can ignore the thread abort
            {}
            catch (Exception ex)
            {
                string msg = string.Format("An error occurred while disconnecting from the data source during the shutdown of the adapter,'{0}'. Exception: {1}", AdapterName, ex.ToString());
                RaiseAdapterInfo(ErrorLevel.Warning, msg);
            }
        }
        #endregion

        /// <summary>
        /// Helper function used to create an instance of the Neuron ESB Audit service so message failures when publishing to the bus can be forwared
        /// to the Neuron Audit database
        /// </summary>
        /// <returns></returns>
        private bool ConnectAuditService()
        {
            bool retVal = false;
            // if auditing is enabled
            if (AuditOnFailurePublish && this._adapterMode.ModeName.Equals(AdapterModeStringConstants.Publish))
            {
                try
                {
                    if (MessageAuditService == null)
                    {
                        MessageAuditService = new MessageAudit();
                        MessageAuditService.Connect(Configuration);
                    }
                    retVal = true;

                }
                catch (Exception ex)
                {
                    RaiseAdapterError(ErrorLevel.Error, "Failed to create Audit Service", ex);
                }
            }

            return retVal;
        }
        #region Publish Functions
        /// <summary>
        /// Started in the Connect method if configured for polling receive mode
        /// </summary>
        /// <remarks>
        /// This should always be executed in a background thread so that things can be cleaned up
        /// simply by exiting the thread
        /// </remarks>
        private void ReceiveThreadRoutine()
        {
            // wait polling interval, unless we're shutting down.  if our threads are background threads we
            // can just exit, no need to cleanup
            
            while (true)
            {
                // call the function that handles calling the external resource, obtaining information and publishing to the bus
               if (TryReceive()) break;

                // wait for design time configured interval before continuing the while loop
                if (this._quitEvent.WaitOne(this.PollingInterval * 1000, false))
                    break;
            }
        }

        /// <summary>
        /// Fired by the receive thread routine. Calls out to user defined function, ReceiveFromSource(), that handles calling the external resource, 
        /// obtaining information and publishing to the bus returns false if stop onerror is selected for errorhandling and an error occurs
        /// </summary>
        private bool TryReceive()
        {
            bool retVal = false;

            try
            {
                // user defined method
                ReceiveFromSource();

                _pollFailCount = 0;
                _reportPollingErrors = true;
            }
            catch (Exception ex)
            {
                if (ErrorHandling == AdapterPublishErrorHandlingEnum.StopPollingOnError) retVal = true;

                if (_reportPollingErrors)
                {
                    HandleMaxPollingExceptions(ex);
                }
            }

            return retVal;
        }

        #endregion

        #region Exception Routines

        /// <summary>
        /// Called from TryReceive()'s error handler to manage the exception message returned, as well as how often and at what level error, warnings or informational messages
        /// are logged regarding failure in the polling function.
        /// </summary>
        /// <param name="ex"></param>
        private void HandleMaxPollingExceptions(Exception ex)
        {
            string msg = string.Format(CultureInfo.InvariantCulture, "A general publishing error occurred {0}", ex.Message);

            // enrich exception message
            var failureMessage = string.Format(
                        CultureInfo.InvariantCulture,
                        msg + System.Environment.NewLine + System.Environment.NewLine + "Adapter Type: {0}" + System.Environment.NewLine + "Party ID: {1}" + System.Environment.NewLine + "Active Deployment Group: {2}",
                        AdapterName ?? "Unknown",
                        base.PartyId ?? "Unknown",
                        Helper.ActiveDeploymentGroup ?? "Unknown");

            if (ErrorHandling == AdapterPublishErrorHandlingEnum.StopPollingOnError)
            {
                msg = string.Format(CultureInfo.InvariantCulture, "{0}" + System.Environment.NewLine + System.Environment.NewLine + "THE ADAPTER WILL NOW BE DISCONNECTED.", failureMessage);
                RaiseAdapterError(GetErrorLevelFromMode(), GetErrorMessageFromException(ex, msg), ex);
            }

            if (ErrorHandling == AdapterPublishErrorHandlingEnum.SuppressConsecutiveErrors)
            {
                _pollFailCount++;
                if (_pollFailCount >= _maxPollFails)
                {
                    msg = string.Format(CultureInfo.InvariantCulture, "{0}" + System.Environment.NewLine + System.Environment.NewLine + "The adapter will NOT be disconnected but error reporting will be suppressed until after a successful poll is executed.", failureMessage);
                    RaiseAdapterError(GetErrorLevelFromMode(), GetErrorMessageFromException(ex, msg), ex);
                    _reportPollingErrors = false;
                }
            }

            if (ErrorHandling == AdapterPublishErrorHandlingEnum.ReportAllErrors)
            {
                RaiseAdapterError(GetErrorLevelFromMode(), GetErrorMessageFromException(ex, failureMessage), ex);
            }
        }

        /// <summary>
        /// Returns the error information to log via the RaiseAdapterError() call.  If Error Mode is informational or warning, the custom 
        /// message + exception stack is returned.  If the error mode is Error, then the message is just returned as the RaiseAdapterError() call will
        /// use the exception object passed to it to write out the exception details.
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        private string GetErrorMessageFromException(Exception ex, string message)
        {
            StringBuilder sb = new StringBuilder();

            if (ErrorMode == AdapterPublishErrorModesEnum.Information || ErrorMode == AdapterPublishErrorModesEnum.Warning)
            {
                sb.AppendLine(message);
                Exception temp = ex.InnerException;
                while (temp != null)
                {
                    sb.AppendLine();
                    sb.AppendFormat("Inner Exception: {0}", temp.Message);
                    temp = temp.InnerException;
                }
                sb.AppendLine();
                sb.AppendLine("Stack Trace:");
                sb.AppendLine(ex.StackTrace);
            }
            else
                sb.AppendLine(message);

            return sb.ToString();
        }
        /// <summary>
        /// Returns the Neuron ESB specific error level for the adapterbase's RaiseAdapterError() call which is consistent with the 
        /// selected ErrorMode property of the adapter
        /// </summary>
        /// <returns></returns>
        private Neuron.Esb.ErrorLevel GetErrorLevelFromMode()
        {
            Neuron.Esb.ErrorLevel errorLevel = ErrorLevel.Error;
            if (ErrorMode == AdapterPublishErrorModesEnum.Information)
                errorLevel = ErrorLevel.Info;
            else if (ErrorMode == AdapterPublishErrorModesEnum.Warning)
                errorLevel = ErrorLevel.Warning;
            else
                errorLevel = ErrorLevel.Error;

            return errorLevel;
        }
        
        
        
        #endregion
    }
}
