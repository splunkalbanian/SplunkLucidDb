package com.splunk.search;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.*;

import net.sf.farrago.trace.FarragoTrace;
import au.com.bytecode.opencsv.CSVReader;

import com.splunk.util.HTTPUtils;
import com.splunk.util.StringUtils;

import static com.splunk.util.HTTPUtils.*;

public class Connection 
{
    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(Connection.class);

    static Pattern SESSION_KEY = Pattern.compile("<response>\\s*<sessionKey>([0-9a-f]+)</sessionKey>\\s*</response>");

    URL url;
    String username, password, sessionKey;
    Map<String, String> requestHeaders = new HashMap<String, String>();
    
    public Connection(String url, String username, String password) throws MalformedURLException {
        this(new URL(url), username, password);
    }
    
    public Connection(URL url, String username, String password) {
        this.url      = url;
        this.username = username;
        this.password = password;
        connect();
    }
     
    private static void close(Closeable c){
        try{c.close();}catch(Exception ignore){}
    }
    
    private void connect() 
    {
        BufferedReader rd = null;
        
        try {
            String loginUrl = 
            String.format("%s://%s:%d/services/auth/login", url.getProtocol(), url.getHost(), 
                          url.getPort());

            StringBuilder data = new StringBuilder();
            appendURLEncodedArgs(data, "username", username, "password", password);

            rd = new BufferedReader(new InputStreamReader(POST(loginUrl, data, requestHeaders)));

            String line;
            StringBuilder reply = new StringBuilder();
            while ((line = rd.readLine()) != null) {
                reply.append(line);
                reply.append("\n");
            }
            
            Matcher m = SESSION_KEY.matcher(reply);
            if(m.find()){
                sessionKey = m.group(1);
                requestHeaders.put("Authorization", "Splunk " + sessionKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            close(rd);
        }
    }
    
    public void getSearchResults(String search, Map<String, String> otherArgs, SearchResultListener srl){
        String searchUrl = 
            String.format("%s://%s:%d/services/search/jobs/export", url.getProtocol(), url.getHost(), 
                          url.getPort());
  
        StringBuilder data = new StringBuilder();
        Map<String, String> args = new HashMap<String, String>();
        if(otherArgs != null)
            args.putAll(otherArgs);
        args.put("search", search);
        // override these args
        args.put("output_mode", "csv");
        args.put("preview", "0");
        
        //TODO: remove this once the csv parser can handle leading spaces
        args.put("check_connection", "0");
        
        appendURLEncodedArgs(data, args);
        try {
            // wait at most 30 minutes for first result
            parseResults(POST(searchUrl, data, requestHeaders, 10000, 1800000), srl );
        }catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            gLogger.warning(e.getMessage() + "\n" + sw);
        }
    }
    
    private static void parseResults(InputStream in, SearchResultListener srl) throws IOException
    {
        CSVReader csvr   = new CSVReader(new InputStreamReader(in));
        try {
            String [] header = csvr.readNext();

            if (header != null && header.length > 0&& !(header.length==1 && header[0].isEmpty())) 
            {
                srl.setFieldNames(header);
                
                String[] line;
                while ((line = csvr.readNext()) != null) {
                    if(line.length == header.length)
                       srl.processSearchResult(line); 
                }
            }
        } catch (IOException ignore) {
            StringWriter sw = new StringWriter();
            ignore.printStackTrace(new PrintWriter(sw));
            gLogger.warning(ignore.getMessage() + "\n" + sw);
        }finally{
            HTTPUtils.close(csvr); // CSVReader closes the inputstream too
        }
    }
    
    static class DummySearchResultListener implements SearchResultListener{
        String[] fieldNames = null;
        int resultCount = 0;
        boolean print = false;
        
        public DummySearchResultListener(boolean print) {
            this.print = print;
        }

        public void setFieldNames(String[] fieldNames){
            this.fieldNames = fieldNames;
        }
        
        @Override
        public boolean processSearchResult(String[] values) 
        {
            resultCount++;
            if(print){
                for(int i=0; i<this.fieldNames.length; ++i){
                    System.out.printf("%s=%s\n", this.fieldNames[i], values[i]);
                }
                System.out.println();
            }
            return true;
        }

        public int getResultCount() {
            return resultCount;
        }
    }
    
    public static void parseArgs(String[] args, Map<String, String> map)
    {
        for(int i=0; i<args.length; i++){
            String argName = args[i++];
            String argValue = i<args.length ? args[i] : "";

            if(!argName.startsWith("-"))
                throw new IllegalArgumentException("invalid argument name: " + argName + ". Argument names must start with -");
            
            map.put(argName.substring(1), argValue);
        }
    }
    
    public static void printUsage(String errorMsg){
        System.err.println(errorMsg);
        System.err.println("Usage: java Connection -<arg-name> <arg-value>");
        System.err.println("The following <arg-name> are valid");
        System.err.println("search        - required, search string to execute");
        System.err.println("field_list    - required, list of fields to request, comma delimited");
        System.err.println("uri           - uri to splunk's mgmt port, default: https://localhost:8089");
        System.err.println("username      - username to use for authentication, default: admin");
        System.err.println("password      - password to use for authentication, default: changeme ");
        System.err.println("earliest_time - earliest time for the search, default: -24h");
        System.err.println("latest_time   - latest time for the search, default: now");
        System.err.println("-print        - whether to print results or just the summary");
        
        System.exit(1);
    }
    
    public static void main(String[] args) throws MalformedURLException
    {
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put("uri",           "https://localhost:8089");
        argsMap.put("username",      "admin");
        argsMap.put("password",      "changeme");
        argsMap.put("earliest_time", "-24h");
        argsMap.put("latest_time",   "now");
        argsMap.put("-print",        "true");
        
        parseArgs(args, argsMap);
        
        
        String search = argsMap.get("search"),  field_list = argsMap.get("field_list");
        
        if(search == null){
            printUsage("Missing required argument: search");
        }
        if(field_list == null){
            printUsage("Missing required argument: field_list");
        }
        
        List<String> fieldList = StringUtils.decodeList(field_list, ',');
        
        Connection c = new Connection(argsMap.get("uri"), argsMap.get("username"), argsMap.get("password"));
        
        Map<String, String> searchArgs = new HashMap<String, String>();
        searchArgs.put("earliest_time", argsMap.get("earliest_time"));
        searchArgs.put("latest_time"  , argsMap.get("latest_time"));
        searchArgs.put("field_list"   , StringUtils.encodeList(fieldList, ',').toString());

        
        DummySearchResultListener dummy = new DummySearchResultListener(Boolean.valueOf(argsMap.get("-print")));
        long start = System.currentTimeMillis();
        c.getSearchResults(search, searchArgs, dummy);

        System.out.printf("received %d results in %dms\n", dummy.getResultCount(), (System.currentTimeMillis()-start));
    }
}
