package com.splunk.search;

import java.util.Map;

public interface SearchResultListener 
{
    /**
     * 
     * @param result
     * @return true to continue parsing, false otherwise
     */
    boolean processSearchResult(String[] fieldValues);
 
    
    void setFieldNames(String[] fieldNames);
}
