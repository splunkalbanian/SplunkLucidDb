package com.splunk.udx;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;


/**
 * SplunkUdxRel is an extension of {@link FarragoJavaUdxRel}
 */
class SplunkUdxRel
    extends FarragoJavaUdxRel
{
    // ~ Instance fields -------------------------------------------------------

    protected MedSplunkColumnSet table;
    protected FarragoUserDefinedRoutine udx;

    // ~ Constructors ----------------------------------------------------------

    public SplunkUdxRel(
        RelOptCluster cluster,
        RexNode rexCall,
        RelDataType rowType,
        MedSplunkColumnSet splunkTable,
        FarragoUserDefinedRoutine udx)
    {
        super(cluster, rexCall, rowType, splunkTable.getServerMofId(), RelNode.emptyArray);
        this.table = splunkTable;
        this.udx = udx;
    }

    // ~ Methods ---------------------------------------------------------------

    public MedSplunkColumnSet getTable()
    {
        return this.table;
    }

    public FarragoUserDefinedRoutine getUdx()
    {
        return this.udx;
    }

    public String getServerMofId()
    {
        return this.table.getServerMofId();
    }
}

// End SplunkUdxRel.java
