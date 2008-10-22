package org.apache.pig.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.parser.* ;

public class TestPigScriptParser extends TestCase {

    @Test
    public void testParserWithEscapeCharacters() throws Exception {

        // All the needed variables
        Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
        Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>() ;
        Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>() ;
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties()) ;
        
        String tempFile = this.prepareTempFile() ;
        
        // Start the real parsing job
        {
        	// Initial statement
        	String query = String.format("A = LOAD '%s' ;", Util.encodeEscape(tempFile)) ;
        	ByteArrayInputStream in = new ByteArrayInputStream(query.getBytes()); 
        	QueryParser parser = new QueryParser(in, pigContext, "scope", aliases, opTable, aliasOp) ;
        	LogicalPlan lp = parser.Parse() ; 
        }
        
        {
        	// Normal condition
        	String query = "B1 = filter A by $0 eq 'This is a test string' ;" ;
        	checkParsedConstContent(aliases, opTable, pigContext, aliasOp, 
        	                        query, "This is a test string") ;	
        }
        
        {
        	// single-quote condition
        	String query = "B2 = filter A by $0 eq 'This is a test \\'string' ;" ;
        	checkParsedConstContent(aliases, opTable, pigContext, aliasOp, 
        	                        query, "This is a test 'string") ;	
        }
        
        {
        	// newline condition
        	String query = "B3 = filter A by $0 eq 'This is a test \\nstring' ;" ;
        	checkParsedConstContent(aliases, opTable, pigContext, aliasOp, 
        	                        query, "This is a test \nstring") ;	
        }
        
        {
        	// Unicode
        	String query = "B4 = filter A by $0 eq 'This is a test \\uD30C\\uC774string' ;" ;
        	checkParsedConstContent(aliases, opTable, pigContext, aliasOp, 
        	                        query, "This is a test \uD30C\uC774string") ;	
        }
    }

	private void checkParsedConstContent(Map<LogicalOperator, LogicalPlan> aliases,
                                         Map<OperatorKey, LogicalOperator> opTable,
                                         PigContext pigContext,
                                         Map<String, LogicalOperator> aliasOp,
                                         String query,
                                         String expectedContent)
                                        throws Exception {
        // Run the parser
        ByteArrayInputStream in = new ByteArrayInputStream(query.getBytes()); 
        QueryParser parser = new QueryParser(in, pigContext, "scope", aliases, opTable, aliasOp) ;
        LogicalPlan lp = parser.Parse() ; 
        
        // Digging down the tree
        LogicalOperator root = lp.getRoots().get(0) ;
        LogicalOperator filter = lp.getSuccessors(root).get(0);
        LogicalPlan comparisonPlan = ((LOFilter)filter).getComparisonPlan();
        List<LogicalOperator> comparisonPlanRoots = comparisonPlan.getRoots();
        LogicalOperator compRootOne = comparisonPlanRoots.get(0);
        LogicalOperator compRootTwo = comparisonPlanRoots.get(1);

        
        // Here is the actual check logic
        if (compRootOne instanceof LOConst) {
            assertTrue("Must be equal", 
                        ((String)((LOConst)compRootOne).getValue()).equals(expectedContent)) ;
        } 
        // If not left, it must be right.
        else {
            assertTrue("Must be equal", 
                        ((String)((LOConst)compRootTwo).getValue()).equals(expectedContent)) ;
        }
    }

    private String prepareTempFile() throws IOException {
        File inputFile = File.createTempFile("test", "txt");
        inputFile.deleteOnExit() ;
        PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
        ps.println("hohoho") ;
        ps.close();
        return inputFile.getPath() ;
    }

}
