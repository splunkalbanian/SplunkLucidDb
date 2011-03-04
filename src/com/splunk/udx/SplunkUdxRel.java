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
