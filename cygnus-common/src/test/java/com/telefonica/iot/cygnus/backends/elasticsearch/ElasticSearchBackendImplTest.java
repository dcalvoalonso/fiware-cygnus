package com.telefonica.iot.cygnus.backends.elasticsearch;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.telefonica.iot.cygnus.backends.ckan.CKANBackendImpl;

/***
 * 
 * @author a620381
 *
 */

@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchBackendImplTest {
	
	private ElasticSearchBackendImpl backend;
	
    @Mock
    private HttpClient mockHttpClient;
    
    private final String host = "localhost";
    private final String port = "9200";
    
    private final String bulkCmd = "this is a lot of data";
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        backend = new ElasticSearchBackendImpl(host, port);

        BasicHttpResponse response = new BasicHttpResponse(new ProtocolVersion("http", 1, 1), 200, "ok");
        response.setEntity(new StringEntity("{\"result\": {\"whatever\":\"whatever\"}}"));
        when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class))).thenReturn(response);
    } // setUp
	
    /**
     * Test of persist method, of class CKANBackendImpl.
     */
    @Test
    public void testPersist() {
        System.out.println("Testing ElasticSearchBackendImpl.persist");
        
        try {
            backend.setHttpClient(mockHttpClient);
            backend.persist(bulkCmd);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testPersist

}
