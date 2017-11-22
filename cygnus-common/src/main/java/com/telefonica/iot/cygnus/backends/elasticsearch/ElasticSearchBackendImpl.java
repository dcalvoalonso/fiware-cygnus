package com.telefonica.iot.cygnus.backends.elasticsearch;

import java.util.ArrayList;

import org.apache.http.Header;
import org.apache.http.entity.StringEntity;

import com.telefonica.iot.cygnus.backends.http.HttpBackend;
import com.telefonica.iot.cygnus.backends.http.JsonResponse;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.log.CygnusLogger;

/**
 * Interface for those backends implementing the persistence in ElasticSearch.
 * 
 * @author a620381
 *
 */
public class ElasticSearchBackendImpl extends HttpBackend implements ElasticSearchBackend {
	
    private static final CygnusLogger LOGGER = new CygnusLogger(ElasticSearchBackendImpl.class);
	public ElasticSearchBackendImpl(String elasticSearchHost, String elasticSearchPort) {
		super(elasticSearchHost, elasticSearchPort, false, false, null, null, null, null, 500, 100);

	}

	@Override
	public void persist(String bulkRecords) throws CygnusBadConfiguration, CygnusRuntimeError, CygnusPersistenceError {
    
    // create the ES request URL
    String urlPath = "_bulk";
    ArrayList<Header> headers = new ArrayList<>();
    // do the ES request
    JsonResponse res = doRequest("POST", urlPath, true, headers, new StringEntity(bulkRecords, "UTF-8"));

    // check the status
    if (res.getStatusCode() == 200) {
        LOGGER.debug("ES Bulk operation result=" + res.toString() + ")");
    } else {
        throw new CygnusPersistenceError("Could not execute bulk operation in ES, statusCode="
                + res.getStatusCode() + ")");
    } // if else
		
	}

}
