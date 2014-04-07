package com.michaelmiklavcic.hadoop.pig;

import static org.junit.Assert.*;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.pig.pigunit.PigTest;
import org.junit.Before;
import org.junit.Test;


public class PigUnitUtilTest {

    @Before
    public void setup() throws NotFoundException, CannotCompileException {
        PigUnitUtil.runFix();
    }
    
    @Test
    public void testTop2Queries() throws Exception {
      String[] args = {
          "n=2",
          };
      System.getProperties().setProperty("mapred.map.child.java.opts", "-Xmx1G");
      System.getProperties().setProperty("mapred.reduce.child.java.opts","-Xmx1G");
      System.getProperties().setProperty("io.sort.mb","10");
      PigTest test = new PigTest("src/test/resources/top_queries.pig", args);
   
      String[] input = {
          "yahoo",
          "yahoo",
          "yahoo",
          "twitter",
          "facebook",
          "facebook",
          "linkedin",
      };
   
      String[] output = {
          "(yahoo,3)",
          "(facebook,2)",
      };
      
      test.assertOutput("data", input, "queries_limit", output);
    }

}
