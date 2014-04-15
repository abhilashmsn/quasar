using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Neuron;
using Neuron.Esb;
using Neuron.Esb.Adapters;
using System.Security.Cryptography;
using System.Net;
using System.IO;
using System.ComponentModel;
using System.Transactions;

namespace Quasar.Neuron.Adaptors.Twitter
{
    public class Twitter : ESBAdapterBase
    {
        string _OAuthVersion = "1.0";
        string _OAuthSignatureMethod = "HMAC-SHA1";

        [DisplayName("API Key")]
        [Category("oAuth Settings")]
        public string APIKey { get; set; }

        [DisplayName("API Secret")]
        [Category("oAuth Settings")]
        public string APISecret { get; set; }

        [DisplayName("Access Token")]
        [Category("oAuth Settings")]
        public string AccessToken { get; set; }

        [DisplayName("Access Token Secret")]
        [Category("oAuth Settings")]
        public string AccessTokenSecret { get; set; }

        [DisplayName("URI")]
        [Category("Request Settings")]
        public string RequestURI { get; set; }

        public Twitter()
        {
            AdapterModes = new AdapterMode[]
            {
                new AdapterMode("Subscribe", MessageDirection.DatagramSender)
            };

            Capabilities = new ESBAdapterCapabilities()
            {
                AdapterName = "TweetAdapter"
            };
        }

        public override void Connect(string adapterMode)
        {
            if (string.IsNullOrEmpty(APIKey)) throw new ArgumentNullException("API Key");
            if (string.IsNullOrEmpty(APISecret)) throw new ArgumentNullException("API Secret");
            if (string.IsNullOrEmpty(AccessToken)) throw new ArgumentNullException("Access Token");
            if (string.IsNullOrEmpty(AccessTokenSecret)) throw new ArgumentNullException("Access Token Secret");
            if (string.IsNullOrEmpty(RequestURI)) throw new ArgumentNullException("URI");

            RaiseAdapterInfo(ErrorLevel.Info, string.Format("ADAPTER MODE {0}", adapterMode));
        }

        public override void Disconnect()
        {
            RaiseAdapterInfo(ErrorLevel.Info, "DISCONNECT");
        }

        public override void SendToEndpoint(global::Neuron.Esb.ESBMessage message, CommittableTransaction tx)
        {
            RaiseAdapterInfo(ErrorLevel.Info, string.Format("MESSAGE ID: {0} ~ TOPIC: {1}", message.Header.MessageId, message.Header.Topic));

            TweetStatus(message);
        }

        private void TweetStatus(ESBMessage message)
        {
            try
            {
                string status = message.ToString();
                var postBody = "status=" + Uri.EscapeDataString(status);
                RaiseAdapterInfo(ErrorLevel.Info, status);

                using (HMACSHA1 hasher = new HMACSHA1(ASCIIEncoding.ASCII.GetBytes(string.Format("{0}&{1}", Uri.EscapeDataString(APISecret), Uri.EscapeDataString(AccessTokenSecret)))))
                {
                    string _oauthNonce = Convert.ToBase64String(new ASCIIEncoding().GetBytes(DateTime.Now.Ticks.ToString()));
                    TimeSpan _timeSpan = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                    string _oauthTimestamp = Convert.ToInt64(_timeSpan.TotalSeconds).ToString();

                    string baseString = string.Format("oauth_consumer_key={0}&oauth_nonce={1}&oauth_signature_method={2}&oauth_timestamp={3}&oauth_token={4}&oauth_version={5}&status={6}",
                                APIKey, _oauthNonce, _OAuthSignatureMethod, _oauthTimestamp, AccessToken, _OAuthVersion, Uri.EscapeDataString(status));

                    string _oauthsignature = Convert.ToBase64String(hasher.ComputeHash(ASCIIEncoding.ASCII.GetBytes(string.Concat("POST&", Uri.EscapeDataString(RequestURI), "&", Uri.EscapeDataString(baseString)))));

                    string _authenticationHeader = string.Format("OAuth oauth_consumer_key=\"{0}\", oauth_nonce=\"{1}\", oauth_signature=\"{2}\", oauth_signature_method=\"{3}\", oauth_timestamp=\"{4}\", oauth_token=\"{5}\", oauth_version=\"{6}\"",
                                        Uri.EscapeDataString(APIKey), Uri.EscapeDataString(_oauthNonce), Uri.EscapeDataString(_oauthsignature), Uri.EscapeDataString(_OAuthSignatureMethod), Uri.EscapeDataString(_oauthTimestamp), Uri.EscapeDataString(AccessToken), Uri.EscapeDataString(_OAuthVersion));

                    ServicePointManager.Expect100Continue = false;
                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(RequestURI);
                    request.Headers.Add("Authorization", _authenticationHeader);
                    request.Method = "POST";
                    request.ContentType = "application/x-www-form-urlencoded";
                    using (Stream stream = request.GetRequestStream())
                    {
                        byte[] content = ASCIIEncoding.ASCII.GetBytes(postBody);
                        stream.Write(content, 0, content.Length);
                        WebResponse response = request.GetResponse();
                        using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                        {
                            string result = reader.ReadToEnd();
                        }
                        response.Close();
                    }
                }
            }
            catch (WebException ex)
            {
                WebResponse response = ex.Response;
                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                {
                    string result = reader.ReadToEnd();
                }
                response.Close();
                RaiseAdapterError(ErrorLevel.Error, "Tweet Status Failed", ex);
            }
            catch (Exception ex)
            {
                RaiseAdapterError(ErrorLevel.Error, "Unknown Error", ex);
            }
            finally
            {
            }
        }
    }
}
