package com.telefonica.iot.cygnus.sinks;

import static com.telefonica.iot.cygnus.utils.CommonUtilsForTests.getTestTraceHead;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flume.Context;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.telefonica.iot.cygnus.sinks.Enums.DataModel;

@RunWith(MockitoJUnitRunner.class)
public class NGSIElasticSearchSinkTest {
	
    /**
     * Constructor.
     */
    public NGSIElasticSearchSinkTest() {
        LogManager.getRootLogger().setLevel(Level.FATAL);
    } // NGSICKANSinkTest
    

    /**
     * [NGSICKANSink.configure] -------- When not configured, not mandatory parameters get default values.
     */
    @Test
    public void testConfigureDefaults() {
        System.out.println(getTestTraceHead("[NGSIElasticSearchSinkTest.configure]")
                + "-------- When not configured, not mandatory parameters get default values");

        String host = null; // default
        String port = null; // default
        String index = null; // default
        String type = null; // default
        NGSIElasticSearchSink sink = new NGSIElasticSearchSink();
        sink.configure(createContext(host, port, index, type));

        try {
            assertEquals(1, sink.getBatchSize());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'batch_size=1' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'batch_size=1' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals(30, sink.getBatchTimeout());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'batch_timeout=30' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'batch_timeout=30' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals(10, sink.getBatchTTL());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'batch_ttl=30' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'batch_ttl=30' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals(DataModel.DMBYENTITY, sink.getDataModel());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'data_model=dm-by-entity' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'data_model=dm-by-entity' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertTrue(!sink.getEnableEncoding());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'enable_encoding=false' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'enable_encoding=false' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertTrue(!sink.getEnableGrouping());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'enable_grouping=false' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'enable_grouping=false' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertTrue(sink.getEnableLowerCase());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'enable_lowercase=true' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'enable_lowercase=true' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals("localhost", sink.getElasticSearchHost());
            System.out.println(getTestTraceHead("[NGSINGSIElasticSearchSinkCKANSink.configure]")
                    + "-  OK  - 'es_host=localhost' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_host=localhost' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals("9200", sink.getElasticSearchPort());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_port=9200' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_port=9200' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals("index", sink.getElasticSearchIndex());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_index=index' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_index=localhost' not configured by default");
            throw e;
        } // try catch
  
        try {
            assertEquals("type", sink.getElasticSearchType());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_type=type' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_type=type' not configured by default");
            throw e;
        } // try catch
       
    } // testConfigureDefaults
    
    /**
     * [NGSICKANSink.configure] -------- Parameters get the configured value.
     */
    @Test
    public void testConfigureGetConfiguration() {
        System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                + "-------- Parameters gets the configured value");
        String host = "devel-u-02.fuse.ovh"; // default
        String port = "9300"; // default
        String index = "test_index"; // default
        String type = "test_type"; // default
        NGSIElasticSearchSink sink = new NGSIElasticSearchSink();
        sink.configure(createContext(host, port, index, type));

        try {
            assertEquals(host, sink.getElasticSearchHost());
            System.out.println(getTestTraceHead("[NGSINGSIElasticSearchSinkCKANSink.configure]")
                    + "-  OK  - 'es_host="+host+"' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_host="+host+"' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals(port, sink.getElasticSearchPort());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_port="+port+"' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_port="+port+"' not configured by default");
            throw e;
        } // try catch
        
        try {
            assertEquals(index, sink.getElasticSearchIndex());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_index="+index+"' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_index="+index+"' not configured by default");
            throw e;
        } // try catch
  
        try {
            assertEquals(type, sink.getElasticSearchType());
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "-  OK  - 'es_type="+type+"' configured by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSIElasticSearchSink.configure]")
                    + "- FAIL - 'es_type="+type+"' not configured by default");
            throw e;
        } // try catch
        
    } // testConfigureGetConfiguration
    
    private Context createContext(String host, String port, String index,
            String type) {
        Context context = new Context();
        context.put("es_host", host);
        context.put("es_port", port);
        context.put("es_index", index);
        context.put("es_type", type);
        return context;
    } // createContext
}
