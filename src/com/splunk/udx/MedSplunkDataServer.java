package com.splunk.udx;

import java.sql.*;

import java.util.*;
import java.util.logging.Logger;

import javax.sql.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.FarragoJavaUdxRel;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.FarragoTrace;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * MedSplunkDataServer provides an implementation of the {@link
 * FarragoMedDataServer} interface for Splunk.
 *
 */
class MedSplunkDataServer
    extends MedAbstractDataServer
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_UDX_SPECIFIC_NAME = "UDX_SPECIFIC_NAME";

    public static final String PROP_URL         = "SPLUNK_SERVER";
    public static final String DEFAULT_URL      = "https://localhost:8089";

    public static final String PROP_USERNAME    = "USERNAME";
    public static final String DEFAULT_USERNAME = "admin";

    public static final String PROP_PASSWORD    = "PASSWORD";
    public static final String DEFAULT_PASSWORD = "changeme";


    public static final String DEFAULT_UDX_SPECIFIC_NAME
        = "LOCALDB.SPLUNK.SPLUNK_SEARCH";

    //~ Instance fields --------------------------------------------------------

    private MedAbstractDataWrapper wrapper;

    private String url, username, password;

    //~ Constructors -----------------------------------------------------------

    MedSplunkDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
        this.wrapper = wrapper;
    }

    //~ Methods ----------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        
        url      = props.getProperty(PROP_URL, DEFAULT_URL);
        username = props.getProperty(PROP_USERNAME, DEFAULT_USERNAME);
        password = props.getProperty(PROP_PASSWORD, DEFAULT_PASSWORD);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map<String, Properties> columnPropMap)
        throws SQLException
    {
        String udxSpecificName = getProperties().getProperty(
            PROP_UDX_SPECIFIC_NAME, DEFAULT_UDX_SPECIFIC_NAME);
        
        return new MedSplunkColumnSet(
            this,
            tableProps,
            localName,
            rowType,
            udxSpecificName);
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);

        // pushdown rules

        // case 1: projection on top of a filter (with push down projection)
        // ie: filtering on variables which are not in projection
        planner.addRule(  SplunkPushDownRule.getProjOnFilterOnProj(this) );

        // case 2: filter with push down projection
        // ie: proj only has values which are already in filter expression
        planner.addRule( SplunkPushDownRule.getFilterOnProj(this) );

        
        // case 3: filter with no projection to push down.
        // ie: select * where [filter]
        planner.addRule( SplunkPushDownRule.getFilter(this) );

        // case 4: only projection, no filter
        // "proj"
        planner.addRule(SplunkPushDownRule.getProjection(this) );       

        // case 5: add aggregation rules
        planner.addRule(new MedSplunkAggPushDownRule(this));
    }

    MedAbstractDataWrapper getWrapper()
    {
        return wrapper;
    }

    public String getUrl()
    {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
