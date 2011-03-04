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

package com.splunk.udx;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import com.splunk.search.*;
import com.splunk.util.StringUtils;

import java.util.logging.*;

import net.sf.farrago.trace.*;

public abstract class SplunkUdx
{
    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(SplunkUdx.class);
    
    static class SplunkSearchResultListener implements SearchResultListener
    {
        PreparedStatement ps;
        ParameterMetaData pmd;
        Map<String, Integer> fields;
        int resultCount = 0;
        
        
        SplunkSearchResultListener(PreparedStatement ps, List<String> fields) throws SQLException{
            this.ps     = ps;
            this.fields = new HashMap<String, Integer>();
            
            int i = 0;
            for (String field : fields) {
                this.fields.put(field.toUpperCase(), new Integer(i++));
            }
            
            this.pmd = this.ps.getParameterMetaData();
        }
        
        int fieldValueIndexes[] = null;
        @Override
        public void setFieldNames(String[] fieldNames){
            fieldValueIndexes = new int[fields.size()];
            Arrays.fill(fieldValueIndexes, -1);
            for(int i=0;i<fieldNames.length;++i){
                String fn = fieldNames[i].trim();
                Integer idx = fields.get(fn.toUpperCase());
                if(idx != null){
                    fieldValueIndexes[idx] = i;
                }
            }
        }
        
        @Override
        public boolean processSearchResult(String[] fieldValues) 
        {
            if(fieldValues == null) // sanity
                return true;

            resultCount++;
            try 
            {
                if(fieldValueIndexes == null){
                    throw new IllegalStateException("need to call setFieldNames before calling processSearchResult");
                }
                
                //add only the required fields 
                for (int i = 0; i < fieldValueIndexes.length; i++)
                {
                    int idx = fieldValueIndexes[i];
                    if(idx >= fieldValues.length || idx < 0)
                        continue; // should we throw? it's a bad result
                    
                    if(idx > -1){
                        int ps_idx = i+1, pType = this.pmd.getParameterType(ps_idx);
                        String v = fieldValues[idx];
                        
                        switch(pType){
                            case Types.VARCHAR:
                                ps.setString(ps_idx, v);
                                break;
                            case Types.BIGINT:
                                if(!v.isEmpty())
                                    ps.setLong(ps_idx, Long.parseLong(v));
//                                    ps.setBigDecimal(ps_idx, new BigDecimal(new BigInteger(v)));
                                break;
                            case Types.INTEGER:
                                if(!v.isEmpty()) ps.setInt(ps_idx, Integer.parseInt(v));
                                break;
                            case Types.SMALLINT:
                                if(!v.isEmpty()) ps.setShort(ps_idx, Short.parseShort(v));
                                break;
                            case Types.FLOAT:
                                if(!v.isEmpty()) ps.setFloat(ps_idx, Float.parseFloat(v));
                                break;
                            case Types.DOUBLE:
                                if(!v.isEmpty()) ps.setDouble(ps_idx, Double.parseDouble(v));
                                break;
                            case Types.BOOLEAN:
                                if(!v.isEmpty()) ps.setBoolean(ps_idx, StringUtils.parseBoolean(v, false, false));
                                break;
                            default:
                                throw new IllegalArgumentException("Unsupported column type: " + this.pmd.getParameterTypeName(ps_idx));
                        }
                    }
                }
                
                try{
                    // if this throws it means we're done
                    ps.executeUpdate();
                }catch (SQLException ex) {
                    StringWriter sw = new StringWriter();
                    ex.printStackTrace(new PrintWriter(sw));
                    gLogger.warning(ex.getMessage() + "\n" + sw);
                    return false;
                }
            
            } catch (SQLException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                gLogger.warning(e.getMessage() + "\n" + sw);
            }
            return true;
        }

        public int getResultCount() {
            return resultCount;
        }
        
    }

    public static void search(String search, String earliest, String latest, String url, String username, String password, String fields, PreparedStatement resultInserter)  throws SQLException
    {
        try 
        {
            List<String> fieldList = StringUtils.decodeList(fields, '|');
            Connection c = new Connection(url, username, password);
            
            Map<String, String> args = new HashMap<String, String>();
            args.put("earliest_time", earliest);
            args.put("latest_time", latest);
            
            //TODO: use f instead of field_list
	    args.put("field_list", StringUtils.encodeList(fieldList, ',').toString());

	    gLogger.info(String.format("url=\"%s\", earliest=\"%s\", latest=\"%s\", fields=\"%s\", search=\"%s\"", url, earliest, latest, StringUtils.encodeList(fieldList, ',').toString(), search));

	    long start = System.currentTimeMillis();
	    SplunkSearchResultListener ssrl = new SplunkSearchResultListener(resultInserter, fieldList);
            c.getSearchResults(search, args, ssrl);
            
            gLogger.info(String.format("received result_count=%d, in elapsed_time=%dms", 
                         ssrl.getResultCount(), (System.currentTimeMillis()-start)) );
        } catch (MalformedURLException e) {
		throw new SQLException(e.getMessage());
        }
    }

}

