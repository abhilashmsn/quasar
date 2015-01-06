using System;
using System.Collections.Generic;
using System.ComponentModel;
using Neuron.Esb.Internal;
using System.Windows.Forms;
using Neuron.Esb.Administration;
using Neuron.Explorer;
using System.Globalization;

namespace Neuron.Esb.Adapters
{
    /// <summary>
    /// Sample type converter demonstrates how to access the Neuron ESB Configuration at design time to show a list of 
    /// entities for user selection within the adapter's property grid. The CertificateConverter() class returns a list of 
    /// server certificates stored in the currently loaded configuration within the Neuron Explorer. 
    /// </summary>
    public class CertificateConverter : StringConverter
    {
        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        public override StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
      
            List<string> list = new List<string>();
            try
            {
                // This obtains a reference to some of the variables from the Neuron Explorer via injection
                var formMain = ServiceLocator.Get<IFormMain>();

                // get creds
                SerializableDictionary<string, ESBCredential> idList = formMain.Configuration.Credentials;

                // loop through and add to array
                if (idList != null)
                {
                    foreach (KeyValuePair<string, ESBCredential> kvp in idList)
                    {
                        // add to new string array
                        if (!kvp.Value.IsSystemObject && kvp.Value.Type == CredentialType.ServerCert)
                        {
                            list.Add(kvp.Value.Name);
                            
                        }
                    }
                }
                list.Sort(System.StringComparer.CurrentCulture);

                return new StandardValuesCollection(list);
                   
            }
            catch (System.ServiceModel.EndpointNotFoundException)
            {
                    MessageBox.Show( string.Format(CultureInfo.InvariantCulture,"Exception occurred while retrieveing the list of Server Certificates from the \r\nNeuron ESB Server. Please confirm that the Neuron ESB service \r\nis started."), string.Format(CultureInfo.InvariantCulture,"Adapter Property Configuration Error"), MessageBoxButtons.OK, MessageBoxIcon.Error);

            }
            catch (Exception ex)
            {
                    MessageBox.Show(ex.Message, string.Format(CultureInfo.InvariantCulture,"Adapter Property Configuration Error"), MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

            return new StandardValuesCollection(list);
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            return true;
        }

    }
}
