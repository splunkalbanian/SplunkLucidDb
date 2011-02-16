package com.splunk.udx;

import java.math.*;
import java.net.URLEncoder;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.FarragoCatalogUtil;
import net.sf.farrago.namespace.FarragoMedColumnSet;
import net.sf.farrago.namespace.impl.MedAbstractColumnSet;
import net.sf.farrago.plugin.FarragoAbstractPluginBase;
import net.sf.farrago.query.FarragoUserDefinedRoutine;
import net.sf.farrago.resource.FarragoResource;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.util.*;

import com.splunk.util.HTTPUtils;
import com.splunk.util.StringUtils;


/**
 * MedSplunkColumnSet provides an implementation of the {@link
 * FarragoMedColumnSet} interface for MQL.
 *
 * @author Ledion Bitincka
 */
class MedSplunkColumnSet
    extends MedAbstractColumnSet
{

    public static final String PROP_SEARCH      = "BASE_SEARCH";
    public static final String DEFAULT_SEARCH   = "search *";

    public static final String PROP_LATEST      = "LATEST";
    public static final String DEFAULT_LATEST   = "now";

    public static final String PROP_EARLIEST    = "EARLIEST";

    final MedSplunkDataServer server;
    final String udxSpecificName;

    final String search, earliest, latest, fields;
    List<String> fieldList;
    
    MedSplunkColumnSet(
        MedSplunkDataServer server,
        Properties tableProps,
        String [] localName,
        RelDataType rowType,
        String udxSpecificName)
    {
        super(localName, null, rowType, null, null);
        this.server = server;
        this.udxSpecificName = udxSpecificName;
        
        FarragoAbstractPluginBase.
        requireProperty(tableProps, PROP_EARLIEST);
        
        search   = tableProps.getProperty(PROP_SEARCH, DEFAULT_SEARCH);
        latest   = tableProps.getProperty(PROP_LATEST, DEFAULT_LATEST);
        earliest = tableProps.getProperty(PROP_EARLIEST);

        this.fieldList = new LinkedList<String>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            this.fieldList.add(field.getName());
        }
        fields = StringUtils.encodeList(fieldList, '|').toString();

    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode rnSearch   = rexBuilder.makeLiteral(this.search);
        RexNode rnEarliest = rexBuilder.makeLiteral(this.earliest);
        RexNode rnLatest   = rexBuilder.makeLiteral(this.latest);

        RexNode rnUrl       = rexBuilder.makeLiteral(server.getUrl());
        RexNode rnUsername  = rexBuilder.makeLiteral(server.getUsername());
        RexNode rnPassword  = rexBuilder.makeLiteral(server.getPassword());

       
        RexNode rnFields   = rexBuilder.makeLiteral(this.fields);

        // Call to super handles the rest.
        return toUdxRel(
            cluster,
            connection,
            udxSpecificName,
            server.getServerMofId(),
            new RexNode[] { rnSearch, rnEarliest, rnLatest, rnUrl, rnUsername, rnPassword, rnFields });
        
    }

    // override FarragoMedColumnSet
    // return a SplunkUdxRel instead of a FarragoJavaUdxRel
    protected RelNode toUdxRel(
        RelOptCluster cluster,
        RelOptConnection connection,
        String udxSpecificName,
        String serverMofId,
        RexNode [] args)
    {
        // Parse the specific name of the UDX.
        SqlIdentifier udxId;
        try {
            SqlParser parser = new SqlParser(udxSpecificName);
            SqlNode parsedId = parser.parseExpression();
            udxId = (SqlIdentifier) parsedId;
        } catch (Exception ex) {
            // FIXME schoi 16-Sep-2010: generate resources
            // throw FarragoResource.instance().MedInvalidUdxId.ex(
            // udxSpecificName,
            // ex);
            return null;
        }

        // Look up the UDX in the catalog.
        List list =
            getPreparingStmt().getSqlOperatorTable().lookupOperatorOverloads(
                udxId,
                SqlFunctionCategory.UserDefinedSpecificFunction,
                SqlSyntax.Function);
        FarragoUserDefinedRoutine udx = null;
        if (list.size() == 1) {
            Object obj = list.iterator().next();
            if (obj instanceof FarragoUserDefinedRoutine) {
                udx = (FarragoUserDefinedRoutine) obj;
                if (!FarragoCatalogUtil.isTableFunction(udx.getFemRoutine())) {
                    // Not a UDX.
                    udx = null;
                }
            }
        }
        if (udx == null) {
            // FIXME schoi 16-Sep-2010: generate resources
            // throw FarragoResource.instance().MedUnknownUdx.ex(udxId.toString());
            return null;
        }

        // UDX wants all types nullable, so construct a corresponding
        // type descriptor for the result of the call.
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType resultType =
            typeFactory.createTypeWithNullability(
                this.rowType,
                true);

        // Create a relational algebra expression for invoking the UDX.
        RexNode rexCall = rexBuilder.makeCall(udx, args);

        RelNode udxRel =
            new SplunkUdxRel(
                cluster,
                rexCall,
                resultType,
                this,
                udx);

        // Optimizer wants us to preserve original types,
        // so cast back for the final result.
        return RelOptUtil.createCastRel(udxRel, this.rowType, true);
    }

    public List<String> getFieldList(){
        return this.fieldList;
    }
    
    public String getServerMofId(){
        return this.server.getServerMofId();
    }
    
}

