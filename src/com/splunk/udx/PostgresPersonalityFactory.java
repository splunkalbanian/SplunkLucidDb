package com.splunk.udx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.*;
import java.util.Map;
import java.util.logging.Logger;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.DelegatingInvocationHandler;


import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.FarragoTrace;

public class PostgresPersonalityFactory implements FarragoSessionPersonalityFactory
{
    private static final Logger gLogger  =
        FarragoTrace.getClassTracer(PostgresPersonalityFactory.class);

    @Override
    public FarragoSessionPersonality newSessionPersonality(FarragoSession session, 
                                                           FarragoSessionPersonality defaultPersonality) {
        
        // create a delegating proxy, overriding only the methods
        // we're interested in
        return (FarragoSessionPersonality)
            Proxy.newProxyInstance(
                    PostgresPersonalityFactory.class.getClassLoader(),
                    new Class[] {FarragoSessionPersonality.class},
                    new PostgresPersonality(defaultPersonality));
    }
    
    public static class PostgresSqlValidator extends FarragoSqlValidator
    {
        public PostgresSqlValidator(FarragoPreparingStmt preparingStmt){
                super(preparingStmt, SqlConformance.Default);
        }
        
        @SuppressWarnings("unchecked")
        protected void validateGroupClause(SqlSelect select)
        {
            gLogger.fine("in validateGroupClause");
            SqlNodeList groupList = select.getGroup();
            if (groupList == null) {
                return;
            }

            // handle ordinals in the GROUP BY clause
            SqlNodeList selectList = select.getSelectList();
            int i = 0, replaced = 0;
            for(SqlNode groupItem : groupList){
               try{
                  int ordinal = Integer.parseInt(groupItem.toString()) - 1;
                  SqlNode selectNode = selectList.get(ordinal);
                  // handle aliasing in select
                  if(selectNode.getKind() == SqlKind.AS){
                      selectNode = ((SqlCall)selectNode).getOperands()[0];
                  }
                  groupList.set(i, (SqlNode)selectNode.clone());
                  replaced++;
               }catch(NumberFormatException ignore){}
               ++i;
            }

            // change the selectScope so that it can now use the replaced ordinals
            // since SqlValidatorImpl keeps selectScopes as private and AggregatingSelectScope
            // does not have a public constructor we use reflection magic :)
            // NOTE: WE ARE ACCESSING PRIVATE OR PACKAGE PROTECTED FIELDS/METHODS HERE
            if(replaced > 0) 
            {
                try{
                     SqlValidatorScope selectScope = getGroupScope(select);
                     Constructor<?> constructors[] = AggregatingSelectScope.class.getDeclaredConstructors();
                     
                     AggregatingSelectScope aggScope = null;
                     for(Constructor<?> constructor : constructors){
                         constructor.setAccessible(true);
                         if(constructor.getParameterTypes().length == 3)
                             aggScope = (AggregatingSelectScope)constructor.newInstance(selectScope, select, false);
                     }
                     
                     Field field = SqlValidatorImpl.class.getDeclaredField("selectScopes");
                     field.setAccessible(true);
                     ((Map<SqlNode, SqlValidatorScope>)field.get(this)).put(select, aggScope);
                }catch(Exception ex){
                    StringWriter sw = new StringWriter();
                    ex.printStackTrace(new PrintWriter(sw));
                    gLogger.warning("Could not translate GROUP BY <ordinal> to GROUP BY <column-name>. SqlValidatorImpl might have changed.\n" + 
                                    ex.getMessage() + "\n" + sw.toString());
                }
            }

            super.validateGroupClause(select);
        }
        
        
    }

    public static class PostgressPreparingStmt extends FarragoPreparingStmt
    {
        public PostgressPreparingStmt(FarragoSessionStmtContext rootStmtContext,
                                      FarragoSessionStmtValidator stmtValidator, 
                                      String sql) {
            super(rootStmtContext, stmtValidator, sql);
            sqlValidator = new PostgresSqlValidator(this);
        }
    }
    
    public static class PostgresPersonality extends DelegatingInvocationHandler
    {
        private final FarragoSessionPersonality defaultPersonality;

        PostgresPersonality(FarragoSessionPersonality defaultPersonality)
        {
            this.defaultPersonality = defaultPersonality;
            gLogger.fine("in PostgresPersonality");
        }

        // implement DelegatingInvocationHandler
        protected Object getTarget()
        {
            return defaultPersonality;
        }
        
//        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable{
//            gLogger.info("in invoke: " + method.getName());
//            return super.invoke(proxy, method, args);
//        }

        public FarragoSessionPreparingStmt newPreparingStmt(FarragoSessionStmtContext stmtContext,
                                                            FarragoSessionStmtValidator stmtValidator){
            return newPreparingStmt(stmtContext, stmtContext, stmtValidator);
        }
        
        public FarragoSessionPreparingStmt newPreparingStmt(FarragoSessionStmtValidator stmtValidator){
            return newPreparingStmt(null, null, stmtValidator);
        }
        
        // copied from FarragoDefaultSessionPersonality
        public FarragoSessionPreparingStmt newPreparingStmt(
                FarragoSessionStmtContext stmtContext,
                FarragoSessionStmtContext rootStmtContext,
                FarragoSessionStmtValidator stmtValidator)
        {
            String sql = (stmtContext == null) ? "?" : stmtContext.getSql().trim();
            gLogger.fine("in newPreparingStmt3: " + sql);
            FarragoPreparingStmt stmt = null;
            
            if(sql.toLowerCase().startsWith("explain ")){
                stmt =  new FarragoPreparingStmt(
                        rootStmtContext,
                        stmtValidator,
                        sql);
            }else{
                stmt =  new PostgressPreparingStmt(
                        rootStmtContext,
                        stmtValidator,
                        sql);
            }
            initPreparingStmt(stmt);
            return stmt;
        }
        
        // copied from FarragoDefaultSessionPersonality
        protected void initPreparingStmt(FarragoPreparingStmt stmt)
        {
            FarragoSessionStmtValidator stmtValidator = stmt.getStmtValidator();
            FarragoSessionPlanner planner =
                stmtValidator.getSession().getPersonality().newPlanner(stmt, true);
            planner.setRuleDescExclusionFilter(
                stmtValidator.getSession().getOptRuleDescExclusionFilter());
            stmt.setPlanner(planner);
        }
    }
    
}