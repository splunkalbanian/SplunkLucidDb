package com.splunk.util;

import java.util.LinkedList;
import java.util.List;

public class StringUtils 
{

    public static StringBuilder encodeList(List<? extends CharSequence> list, char delim){
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (CharSequence cs : list)
        {
            if(!first)
                result.append(delim);
            
            int len = cs.length();
            for(int i=0; i<len; ++i){
                char c = cs.charAt(i);
                if(c == delim)
                    result.append('\\');
                result.append(c);
            }
            first = false;
        }
        return result;
    }
    
    public static List<String> decodeList(CharSequence encoded, char delim){
        List<String> list = new LinkedList<String>();
        
        int len = encoded.length();
        int start = 0, end = 0;
        boolean hasEscapedDelim = false;
        char p = '\0', c = '\0';
        for(int i=0; i<len; i++, ++end){
            p = c;
            c = encoded.charAt(i);
            if(c == delim )
            {
                if(p == '\\'){
                    hasEscapedDelim = true;
                }else{
                    if(!hasEscapedDelim){
                        list.add(encoded.subSequence(start, end).toString());
                    }else{
                        StringBuilder sb = new StringBuilder(end - start);
                        char a='\0', b='\0';
                        for(int j=start; j<end; ++j){
                            b = a;
                            a = encoded.charAt(j);
                            if(b == '\\' && a != delim)
                                sb.append(b);
                            if(a != '\\')
                                sb.append(a);
                        }
                        list.add(sb.toString());
                    }
                    start = end+1;
                    hasEscapedDelim = false;
                }
            }
        }
        
        if(!hasEscapedDelim){
            list.add(encoded.subSequence(start, end).toString());
        }else{
            StringBuilder sb = new StringBuilder(end - start);
            char a='\0', b='\0';
            for(int j=start; j<end; ++j){
                b = a;
                a = encoded.charAt(j);
                if(b == '\\' && a != delim)
                    sb.append(b);
                if(a != '\\')
                    sb.append(a);
            }
            list.add(sb.toString());
        }

        return list;
    }
    
    public static boolean parseBoolean(String str, boolean defaultVal, boolean missingVal)
    {
        if(str == null || str.isEmpty())  
            return missingVal;
        
        if(str.equalsIgnoreCase("t")    || str.equalsIgnoreCase("true") ||
            str.equalsIgnoreCase("yes") || str.equals("1")){
            return true;
        }else if(str.equalsIgnoreCase("f") || str.equalsIgnoreCase("false") ||
            str.equalsIgnoreCase("no")     || str.equals("0")){
            return true;
        }
        return defaultVal; 
    }
    

    public static void main(String[] args){
        List<String> list = new LinkedList<String>();
        list.add("test");
        list.add("test,with,comma");
        list.add("");
        list.add(",");
        

        System.out.println("=============");
        
        StringBuilder sb = encodeList(list, ',');
        System.out.println(sb);
        
        list.clear();
        list = decodeList(sb, ',');
        for(String s : list)
            System.out.println(s);
        
        System.out.println("=============");
        
    }
    
}
