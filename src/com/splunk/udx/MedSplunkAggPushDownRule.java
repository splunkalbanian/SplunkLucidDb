package com.splunk.udx;


import net.sf.farrago.query.FarragoJavaUdxRel;
import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.HepRelVertex;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Logger;

import static com.splunk.udx.SplunkPushDownRule.*;

/**
 *
 * @author Ledion Bitincka
 */
public class MedSplunkAggPushDownRule extends SplunkPushDownRule
{
    // luciddb -> splunk aggregate functions
    public static final Map<String, String> SUPPORTED_FUNCTIONS = new HashMap<String, String>();
    
    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(MedSplunkAggPushDownRule.class);
    
    static{
        SUPPORTED_FUNCTIONS.put("min", "min"); 
        SUPPORTED_FUNCTIONS.put("max", "max"); 
        SUPPORTED_FUNCTIONS.put("sum", "sum"); 
        SUPPORTED_FUNCTIONS.put("avg", "avg"); 
        SUPPORTED_FUNCTIONS.put("count", "count"); 
        SUPPORTED_FUNCTIONS.put("first_value", "first"); 
        SUPPORTED_FUNCTIONS.put("last_value", "last"); 
        SUPPORTED_FUNCTIONS.put("stddev_samp", "stdev"); 
        SUPPORTED_FUNCTIONS.put("stddev_pop", "stdevp"); 
        SUPPORTED_FUNCTIONS.put("var_samp", "var"); 
        SUPPORTED_FUNCTIONS.put("var_pop", "varp"); 
    }
    

    // aggregate on top of udx
    public MedSplunkAggPushDownRule(MedSplunkDataServer server)
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand( SplunkUdxRel.class, ANY)
             ),
             "aggregate",
             server
        );
    }
    
    

  
    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        SplunkUdxRel udxRel = (SplunkUdxRel) call.rels[1];
        
        if (!isSplunkSearchUdx(udxRel)){
            return;
        }
        
        List<RelDataTypeField> fields = udxRel.getRowType().getFieldList();
        List<String> groupBy = new LinkedList<String>();
        List<String> aggFunc = new LinkedList<String>();
        
        gLogger.fine("agg str: " + aggRel.toString());
        
        for (int i = 0; i < aggRel.getGroupCount(); ++i) {
            groupBy.add(fields.get(i).getName());
            gLogger.fine("adding group by: " + fields.get(i).toString() );
        }

        
        for (AggregateCall aggCall : aggRel.getAggCallList()) 
        {
            final List<Integer> argList = aggCall.getArgList();
            String funcName = aggCall.getAggregation().getName().toLowerCase();
            String origFuncName = funcName;
            
            funcName = SUPPORTED_FUNCTIONS.get(funcName);
            gLogger.fine("translated to farrago.funcName=" + origFuncName + " to Splunk.funcName=" + funcName);
            
            // right now we only support single aggreate functions
            if (funcName == null || argList.size() > 1) {
                return;
            }
            
            // get name of the first argument
            String argName = fields.get(argList.get(0)).getName();
            aggFunc.add(funcName + "("  + searchEscape(argName) + ")");
            gLogger.fine("adding aggFunc by: " + funcName + "("  + searchEscape(argName) + ")" );
        }
        
        
        if(aggFunc.isEmpty() || groupBy.isEmpty())
            return;

        // the first bunch of fields are the GROUP BY fields
        RelDataType topRow = aggRel.getRowType();
        gLogger.fine("agg fieldNames: " + getFieldsString(topRow));
        
        int start = groupBy.size(), i=0;
        StringBuilder stats = new StringBuilder("| stats ");
        for(String f : aggFunc){
            if(i++>0)
                stats.append(", ");
            stats.append(f).append(" AS ").append(searchEscape(topRow.getFields()[start++].getName()));
        }
        
        stats.append(" BY ");
        i=0;
        for(String g : groupBy){
            if(i++>0)
                stats.append(", ");
            stats.append(searchEscape(g));
        }

        gLogger.fine("agg str: " + aggRel.toString() + " splunk agg: " + stats.toString());
        // aggregators create fields, so pass its set of fields as the bottom fields, it can also have non-null fields
        call.transformTo(appendSearchString(stats.toString(), udxRel, null, null, topRow, aggRel.getRowType(), true));
    }
}
