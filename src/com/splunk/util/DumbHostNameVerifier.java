package com.splunk.util;

import javax.net.ssl.*;

public class DumbHostNameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String arg0, SSLSession arg1) {
        return true;
    }

}
