package com.splunk.udx;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * MedMqlForeignDataWrapper provides an implementation of the {@link
 * FarragoMedDataWrapper} interface for MQL.
 *
 * @author Ledion Bitincka
 * @version $Id: //open/dev/farrago/ext/mql/src/net/sf/farrago/namespace/mql/MedMqlForeignDataWrapper.java#1 $
 */
public class MedSplunkForeignDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public MedSplunkForeignDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "Splunk_FOREIGN_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        return "Foreign data wrapper for Splunk";
    }

    
    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        MedSplunkDataServer server =
            new MedSplunkDataServer(
                this,
                serverMofId,
                props);
        boolean success = false;
        try {
            server.initialize();
            success = true;
            return server;
        } finally {
            if (!success) {
                server.closeAllocation();
            }
        }
    }
}

