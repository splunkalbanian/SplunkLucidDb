package com.splunk.util;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;

import net.sf.farrago.trace.FarragoTrace;

import com.splunk.udx.SplunkUdx;

public class HTTPUtils 
{
    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(HTTPUtils.class);

    public static HttpsURLConnection getURLConnection(String url) throws MalformedURLException, IOException{
        URLConnection conn = (new URL(url)).openConnection();
        
        // take care of https stuff - most of the time it's only needed to secure client/server comm
        // not to establish the identity of the server
        if(conn instanceof HttpsURLConnection){
            HttpsURLConnection httpsConn = ((HttpsURLConnection)conn);
            httpsConn.setSSLSocketFactory(TrustAllSSLSocketFactory.createSSLSocketFactory());
            httpsConn.setHostnameVerifier(new DumbHostNameVerifier());
        }

        return (HttpsURLConnection)conn;
    }
    
    
    public static void appendURLEncodedArgs(StringBuilder out, Map<String, String> args){
        int i = 0;
        try{
            for(Map.Entry<String, String> me : args.entrySet()){
                if(i++ != 0)
                    out.append("&");
                out.append(URLEncoder.encode(me.getKey(), "UTF-8"));
                out.append("=").append(URLEncoder.encode(me.getValue(), "UTF-8"));
                
            }
        }catch (UnsupportedEncodingException ignore) { }
    }
    
    public static void appendURLEncodedArgs(StringBuilder out, CharSequence ... args){
        if(args.length % 2 != 0)
            throw new IllegalArgumentException("args should contain an even number of items");
        
        try{
            for(int i=0; i<args.length; i+=2){
                if(i != 0)
                   out.append("&");
                out.append(URLEncoder.encode(args[i].toString(), "UTF-8"));
                out.append("=").append(URLEncoder.encode(args[i+1].toString(), "UTF-8"));
            }
        }catch (UnsupportedEncodingException ignore) { }
    }
    
    public static void close(Closeable c){
        try{c.close();}catch(Exception ignore){}
    }

    
    
    public static InputStream POST(String url, CharSequence data, Map<String, String> headers) throws MalformedURLException, IOException{
        return POST(url, data, headers, 10000, 60000);
    }
    
    public static InputStream POST(String url, CharSequence data, Map<String, String> headers, int cTimeout, int rTimeout) throws MalformedURLException, IOException{
        return executeMethod("POST", url, data, headers, cTimeout, rTimeout);
    }
    
    public static InputStream executeMethod(String method, String url, 
                                            CharSequence data, Map<String, String> headers,
                                            int ctimeout, int rtimeout) 
    throws MalformedURLException, IOException{
        HttpsURLConnection conn;
        OutputStreamWriter wr = null;

        try {
            conn = getURLConnection(url);
            conn.setRequestMethod(method);
            conn.setReadTimeout(rtimeout);
            conn.setConnectTimeout(ctimeout);

            if(headers != null){
                for(Map.Entry<String, String> me : headers.entrySet())
                    conn.setRequestProperty(me.getKey(), me.getValue());
            }
            
            if(data != null){
                conn.setDoOutput(true);
                wr = new OutputStreamWriter(conn.getOutputStream());
                wr.write(data.toString());
                wr.flush(); // Get the response
            }
            InputStream in = conn.getInputStream();
            wr.close();

            if(gLogger.isLoggable(Level.FINE))
               gLogger.fine("url: " + url + ", data: " + String.valueOf(data));
            
            return in;
        }finally{
            close(wr);
        }
    }
}
