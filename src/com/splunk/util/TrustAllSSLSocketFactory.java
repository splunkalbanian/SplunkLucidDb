/*******************************************************************************
   Copyright [2011] Splunk Inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
********************************************************************************/

package com.splunk.util;

import javax.net.ssl.*;

import java.io.*;
import java.net.*;

// TODO comment on this class
public class TrustAllSSLSocketFactory extends SocketFactoryImpl {
    private static TrustAllSSLSocketFactory defaultFactory   = new TrustAllSSLSocketFactory();

    private TrustManager[]                  trustAllCerts    = null;
    private SSLContext                      sc               = null;
    private SSLSocketFactory                sslsocketfactory = null;

    protected TrustAllSSLSocketFactory() {

        trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }
        } };
        try {
            sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            sslsocketfactory = sc.getSocketFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#createSocket()
     */
    public Socket createSocket() throws IOException {
        return applySettings(sslsocketfactory.createSocket());
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int)
     */
    public Socket createSocket(InetAddress host, int port) throws java.io.IOException {
        return applySettings(sslsocketfactory.createSocket(host, port));
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int,
     * java.net.InetAddress, int)
     */
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
            throws java.io.IOException {
        return applySettings(sslsocketfactory.createSocket(address, port, localAddress, localPort));
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#createSocket(java.lang.String, int)
     */
    public Socket createSocket(String host, int port) throws java.io.IOException {
        return applySettings(sslsocketfactory.createSocket(host, port));
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#createSocket(java.lang.String, int,
     * java.net.InetAddress, int)
     */
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws java.io.IOException {
        return applySettings(sslsocketfactory.createSocket(host, port, localHost, localPort));
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.net.SocketFactory#getDefault()
     */
    public static TrustAllSSLSocketFactory getDefault() {
        return defaultFactory;
    }

    public static SSLSocketFactory getDefaultSSLSocketFactory() {
        return defaultFactory.sslsocketfactory;
    }

    /**
     * Creates an "accept-all" SSLSocketFactory - ssl sockets will accept ANY
     * certificate sent to them - thus effectively just securing the
     * communications. This could be set in a HttpsURLConnection using
     * HttpsURLConnection.setSSLSocketFactory(.....)
     * 
     * @return SSLSocketFactory
     * @author Ledion Bitincka (Oct 5, 2007)
     */
    public static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory sslsocketfactory = null;
        SSLContext sc = null;
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }
        } };
        try {
            sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            sslsocketfactory = sc.getSocketFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sslsocketfactory;
    }

}