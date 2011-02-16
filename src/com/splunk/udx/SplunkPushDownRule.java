package com.splunk.udx;

import java.util.*;
import java.util.logging.Logger;

import net.sf.farrago.query.*;
import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;

import com.splunk.util.StringUtils;

/**
 * SplunkPushDownRule is a rule to push filters and projections to Splunk
 *
 */
class SplunkPushDownRule
    extends RelOptRule
{
    public static final String UDX_SPECIFIC_NAME = "SPLUNK_SEARCH";

    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(SplunkPushDownRule.class);

    public static Set<SqlKind> SUPPORTED_OPS;
    
 
    static{
        SUPPORTED_OPS = new HashSet<SqlKind>();
        SUPPORTED_OPS.add(SqlKind.EQUALS);
        SUPPORTED_OPS.add(SqlKind.LESS_THAN);
        SUPPORTED_OPS.add(SqlKind.LESS_THAN_OR_EQUAL);
        SUPPORTED_OPS.add(SqlKind.GREATER_THAN);
        SUPPORTED_OPS.add(SqlKind.GREATER_THAN_OR_EQUAL);
        SUPPORTED_OPS.add(SqlKind.NOT_EQUALS);
        SUPPORTED_OPS.add(SqlKind.LIKE);
        SUPPORTED_OPS.add(SqlKind.AND);
        SUPPORTED_OPS.add(SqlKind.OR);
        SUPPORTED_OPS.add(SqlKind.NOT);
    }    

    // ~ Constructors ---------------------------------------------------------
    private MedSplunkDataServer server = null;

    public static SplunkPushDownRule getProjOnFilterOnProj(MedSplunkDataServer server)
    {
        return 
           new SplunkPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class, new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            FilterRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(ProjectRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                                SplunkUdxRel.class) }) }) }),
                "proj on filter on proj", server);
    }

    public static SplunkPushDownRule getFilterOnProj(MedSplunkDataServer server)
    {
        return
          new SplunkPushDownRule(
              new RelOptRuleOperand(
                FilterRel.class, new RelOptRuleOperand[] {
                    new RelOptRuleOperand(ProjectRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                    SplunkUdxRel.class) }) }),
            "filter on proj", server);
    }
    
    public static SplunkPushDownRule getFilter(MedSplunkDataServer server)
    {
        return
          new SplunkPushDownRule(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                            SplunkUdxRel.class)
                }),
            "filter", server);
    }

    public static SplunkPushDownRule getProjection(MedSplunkDataServer server)
    {
        return
          new SplunkPushDownRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                            SplunkUdxRel.class)
                }),
            "proj", server);   
    }
    

    /**
     * Creates a new SplunkPushDownRule object.
     */
    protected SplunkPushDownRule(RelOptRuleOperand rule, String id, MedSplunkDataServer server)
    {
        super(rule, "SplunkPushDownRule: " + id);
        this.server = server;
    }

    // ~ Methods --------------------------------------------------------------

    public boolean isSplunkSearchUdx(SplunkUdxRel udxRel){
        return udxRel.getUdx().getName().equalsIgnoreCase(UDX_SPECIFIC_NAME);
    }
    
    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        gLogger.fine(description);

        int relLength = call.rels.length;
        SplunkUdxRel udxRel = (SplunkUdxRel) call.rels[relLength - 1];
        if (!isSplunkSearchUdx(udxRel))
            return;
        
        FilterRel  filter     = null;
        ProjectRel topProj    = null;
        ProjectRel bottomProj = null;

        
        RelDataType topRow = udxRel.getRowType();
        
        int filterIdx = 2;
        if(call.rels[relLength - 2] instanceof ProjectRel){
            bottomProj = (ProjectRel)call.rels[relLength - 2];
            filterIdx  = 3;
            
            // bottom projection will change the field count/order 
            topRow =  bottomProj.getRowType();
        }

        String filterString = "";
        
        if(filterIdx <= relLength  && call.rels[relLength - filterIdx] instanceof FilterRel){
            filter = (FilterRel) call.rels[relLength - filterIdx];

            int topProjIdx = filterIdx + 1;
            if(topProjIdx <= relLength  && call.rels[relLength - topProjIdx] instanceof ProjectRel){
                topProj = (ProjectRel)call.rels[relLength - topProjIdx];
            }
            
            RexCall filterCall = (RexCall) filter.getCondition();
            SqlOperator     op = filterCall.getOperator();
            RexNode[] operands = filterCall.getOperands();
    
            gLogger.fine("fieldNames: " + getFieldsString(topRow));
            
            filterString = getFilter(op, operands, "", RelOptUtil.getFieldNames(topRow));
    
            if (filterString == null) {
                // can't handle - exit and stop optimizer from calling any SplunkUdxRel related optimizations
                transformToFarragoUdxRel(
                    call,
                    udxRel,
                    filter,
                    topProj,
                    bottomProj);
                return;
            }
        }
        
        // top projection will change the field count/order 
        if(topProj != null)
            topRow =  topProj.getRowType();

        gLogger.fine("pre transformTo fieldNames: " + getFieldsString(topRow));

        call.transformTo(appendSearchString(filterString, udxRel, topProj, bottomProj, topRow, null, false));
    }

    /**
     * 
     * @param toAppend
     * @param udxRel
     * @param topProj
     * @param bottomProj
     * @param fieldNames
     * @param bottomFields
     * @param canHaveNonNullFields - setting this to true will change the udx to FarragoJavaUdxRel, 
     *                               thus optimization rules that require a SplunkUdxRel will  not match anymore
     * @return
     */
    protected RelNode appendSearchString(String toAppend,    SplunkUdxRel udxRel, 
                                         ProjectRel topProj, ProjectRel bottomProj, 
                                         RelDataType topRow, RelDataType bottomRow,
                                         boolean canHaveNonNullFields)
    {
        RexBuilder    rexBuilder = udxRel.getCluster().getRexBuilder();
        RexCall       searchCall = (RexCall) udxRel.getCall();
        RexLiteral searchLiteral = (RexLiteral)
            ((RexCall)((RexCall)(searchCall.getOperands()[0])).getOperands()[0])
            .getOperands()[0];

        String searchStr = ((NlsString) searchLiteral.getValue()).getValue();
        StringBuilder updateSearchStr = new StringBuilder(searchStr);
        
        if(!toAppend.isEmpty())
           updateSearchStr.append(" ").append(toAppend);

        RelDataTypeField[] bottomFields = bottomRow == null ? null : bottomRow.getFields();
        RelDataTypeField[] topFields    = topRow    == null ? null : topRow.getFields();
        
        if(bottomFields == null)
           bottomFields = udxRel.getRowType().getFields();

        // handle bottom projection (ie choose a subset of the table fields)
        if(bottomProj != null){
            RelDataTypeField[] tmp = new RelDataTypeField[ bottomProj.getProjectExps().length];
            int i = 0;
            for(RexNode rn : bottomProj.getProjectExps()){
                RexInputRef rif = (RexInputRef)rn;
                tmp[i++]        = bottomFields[rif.getIndex()];
            }
            bottomFields = tmp;
        }
        
        // field renaming: to -> from
        List < Pair<String, String> > renames = new LinkedList<Pair<String,String>>();
        
        //handle top projection (ie reordering and renaming)
        RelDataTypeField[] newFields    = bottomFields;
        if(topProj != null){
            gLogger.fine("topProj: " + String.valueOf(topProj.getPermutation()));
            newFields = new RelDataTypeField[topFields.length];
            int i = 0;
            for(RexNode rn : topProj.getProjectExps()){
                RexInputRef rif = (RexInputRef)rn;
                RelDataTypeField field = bottomFields[rif.getIndex()];
                if( !bottomFields[rif.getIndex()].getName().equals(topFields[i].getName())){
                    renames.add(new Pair<String, String>(bottomFields[rif.getIndex()].getName(), topFields[i].getName()));
                    field = topFields[i];
                }
                newFields[i++] = field;
            }
        }

        if(!renames.isEmpty()){
            updateSearchStr.append("| rename ");
            for(Pair<String, String> p : renames){
                updateSearchStr.append(p.left).append(" AS ").append(p.right).append(" ");
            }
        }
        
        RelDataType   resultType = new RelRecordType(newFields);
        RexNode searchWithFilter = rexBuilder.makeLiteral(updateSearchStr.toString());
        
        RexNode[] args2 =
            new RexNode[] {
            searchWithFilter,
            ((RexCall) (searchCall.getOperands()[0])).getOperands()[1], // earliest
            ((RexCall) (searchCall.getOperands()[0])).getOperands()[2], // latest
            ((RexCall) (searchCall.getOperands()[0])).getOperands()[3], // url
            ((RexCall) (searchCall.getOperands()[0])).getOperands()[4], // username
            ((RexCall) (searchCall.getOperands()[0])).getOperands()[5], // password
            rexBuilder.makeLiteral(StringUtils.encodeList(getFieldsList(resultType), '|').toString())  // fields
        };

                  
        RelNode rel = null;
        
        // this will stop any further optimizations
        if(canHaveNonNullFields){
            rel = FarragoJavaUdxRel.newUdxRel(
                    FarragoRelUtil.getPreparingStmt(udxRel),
                    resultType,
                    UDX_SPECIFIC_NAME,
                    udxRel.getServerMofId(),
                    args2,
                    RelNode.emptyArray);
        }else{
            RexNode rexCall = rexBuilder.makeCall(udxRel.getUdx(), args2);
            rel = new SplunkUdxRel(
                    udxRel.getCluster(),
                    rexCall,
                    resultType,
                    udxRel.getTable(),
                    udxRel.getUdx());
    
        }

        gLogger.fine("end of appendSearchString fieldNames: " + Arrays.toString(RelOptUtil.getFieldNames(rel.getRowType())));
        return rel;
    }
    
    // ~ Private Methods ------------------------------------------------------
    private static RelNode addProjectionRule(ProjectRel proj, RelNode rel) {
        if (proj == null)
            return rel;

        return new ProjectRel(proj.getCluster(), rel, proj.getProjectExps(), proj.getRowType(), 
                   proj.getFlags(), proj.getCollationList());
    }
    


    //TODO: use StringBuilder instead of String
    //TODO: refactor this to use more tree like parsing,
    //      need to also make sure we use parens properly - currently precedence 
    //      rules are simply left to right
    private String getFilter(
        SqlOperator op,
        RexNode [] operands,
        String s,
        String [] fieldNames)
    {
        if (!valid(op.getKind())) {
            return null;
        }

        // NOT op pre-pended
        if (op.equals(SqlStdOperatorTable.notOperator)) {
            s = s.concat(" NOT ");
        }
        
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof RexCall) {
                s = s.concat("(");
                s = getFilter(
                    ((RexCall) operands[i]).getOperator(),
                    ((RexCall) operands[i]).getOperands(),
                    s,
                    fieldNames);
                if (s == null) {
                    return null;
                }
                s = s.concat(")");
                if (i != (operands.length - 1)) {
                    s = s.concat(" " + op.toString() + " ");
                }
            } else {
                if (operands.length != 2) {
                    return null;
                }
                if (operands[i] instanceof RexInputRef) {
                    if (i != 0) {
                        return null; // must be of form field=value
                    }
                    
                    int fieldIndex = ((RexInputRef) operands[i]).getIndex();
                    String name = fieldNames[fieldIndex];
                    s = s.concat(name);
                } else { // RexLiteral
                    RexLiteral lit = (RexLiteral) operands[i];
                    
                    String tmp = toString(op, lit);
                    if(tmp == null)
                        return null;
                    
                    s = s.concat(tmp);
                }
                if (i == 0) {
                    s = s.concat(toString(op));
                }
            }
        }
        return s;
    }

    private boolean valid(SqlKind kind){
        return SUPPORTED_OPS.contains(kind);
    }

    private String toString(SqlOperator op)
    {
            if (op.equals(SqlStdOperatorTable.likeOperator)) {
                return SqlStdOperatorTable.equalsOperator.toString();
            }else if(op.equals(SqlStdOperatorTable.notEqualsOperator)){
                return "!=";
            }
            return op.toString();
    }
    
    public static String searchEscape(String str)
    {
        if(str.isEmpty())
            return "\"\"";
        
        StringBuilder sb = new StringBuilder(str.length());
        boolean quote = false;
        
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if(c == '"' || c == '\\')
                sb.append('\\');
            sb.append(c);
            
            quote |= !(Character.isLetterOrDigit(c) || c == '_');
        }
        
        if(quote || sb.length() != str.length()){
            sb.insert(0, '"');
            sb.append('"');
            return sb.toString();
        }
        return str;
    }
    
    private String toString(SqlOperator op, RexLiteral literal)
    {
        String value = null;
        SqlTypeName litSqlType = literal.getTypeName();
        if(Arrays.asList(SqlTypeName.numericTypes).contains(litSqlType)){
            value = literal.getValue().toString();
        }else if (litSqlType.equals(SqlTypeName.CHAR)){
            value = ((NlsString) literal.getValue()).getValue().toString();
            if (op.equals(SqlStdOperatorTable.likeOperator)) {
                value = value.replaceAll("%", "*");
            }
            value = searchEscape(value);
        }
        return value;
    }

    // transform the call from SplunkUdxRel to FarragoJavaUdxRel
    // usually used to stop the optimizer from calling us
    protected void transformToFarragoUdxRel(
        RelOptRuleCall call,
        SplunkUdxRel udxRel,
        FilterRel filter,
        ProjectRel topProj,
        ProjectRel bottomProj)
    {
        RelNode rel =
            new FarragoJavaUdxRel(
                udxRel.getCluster(),
                udxRel.getCall(),
                udxRel.getRowType(),
                udxRel.getServerMofId());

        rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

        rel = addProjectionRule(bottomProj, rel);

        if (filter != null) {
            rel =
                new FilterRel(filter.getCluster(), rel, filter.getCondition());
        }

        rel = addProjectionRule(topProj, rel);

        call.transformTo(rel);
    }

    
    public static String getFieldsString(RelDataType row){
        return Arrays.toString(RelOptUtil.getFieldNames(row));
    }
    public static List<String> getFieldsList(RelDataType row){
        return Arrays.asList(RelOptUtil.getFieldNames(row));
    }
    
    
}

// End SplunkPushDownRule.java
