package com.telefonica.iot.cygnus.backends.elasticsearch;

import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;

public interface ElasticSearchBackend {
	
    void persist(String bulkRecords)
            throws CygnusBadConfiguration, CygnusRuntimeError, CygnusPersistenceError;
}
