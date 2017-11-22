package com.telefonica.iot.cygnus.sinks;

import java.util.ArrayList;
import java.util.Date;

import org.apache.flume.Context;
import org.bson.Document;

import com.telefonica.iot.cygnus.backends.ckan.CKANBackendImpl;
import com.telefonica.iot.cygnus.backends.elasticsearch.ElasticSearchBackend;
import com.telefonica.iot.cygnus.backends.elasticsearch.ElasticSearchBackendImpl;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElement;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusCappingError;
import com.telefonica.iot.cygnus.errors.CygnusExpiratingError;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.sinks.Enums.DataModel;
import com.telefonica.iot.cygnus.utils.CommonUtils;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.google.gson.Gson;

/**
 * 
 * @author a620381
 * 
 * ElasticSearch sink for Orion Context Broker.
 *
 */
public class NGSIElasticSearchSink extends NGSISink {
	
    private static final CygnusLogger LOGGER = new CygnusLogger(NGSIElasticSearchSink.class);
    private String elasticSearchHost;
    private String elasticSearchPort;
    private String elasticSearchIndex;
    private String elasticSearchType;
    private ElasticSearchBackend persistenceBackend;
    
    /**
     * Constructor.
     */
    public NGSIElasticSearchSink() {
        super();
    } // NGSIElasticSearchSink

    /**
     * Gets the ElasticSearch host. It is protected due to it is only required for testing purposes.
     * @return The ElasticSearch host
     */
    protected String getElasticSearchHost() {
        return elasticSearchHost;
    } // getElasticSearchHost

    /**
     * Gets the ElasticSearch port. It is protected due to it is only required for testing purposes.
     * @return The ElasticSearch port
     */
    protected String getElasticSearchPort() {
        return elasticSearchPort;
    } // getElasticSearchPort
    
    /**
     * Gets the ElasticSearch index. It is protected due to it is only required for testing purposes.
     * @return The ElasticSearch index
     */
    protected String getElasticSearchIndex() {
        return elasticSearchIndex;
    } // getElasticSearchIndex
    
    /**
     * Gets the ElasticSearch type. It is protected due to it is only required for testing purposes.
     * @return The ElasticSearch type
     */
    protected String getElasticSearchType() {
        return elasticSearchType;
    } // getElasticSearchType
    
    /**
     * Returns the persistence backend. It is protected due to it is only required for testing purposes.
     * @return The persistence backend
     */
    protected ElasticSearchBackend getPersistenceBackend() {
        return persistenceBackend;
    } // getPersistenceBackend

    /**
     * Sets the persistence backend. It is protected due to it is only required for testing purposes.
     * @param persistenceBackend
     */
    protected void setPersistenceBackend(ElasticSearchBackend persistenceBackend) {
        this.persistenceBackend = persistenceBackend;
    } // setPersistenceBackend
    
    @Override
    public void configure(Context context) {
        elasticSearchHost = context.getString("es_host", "localhost");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (es_host=" + elasticSearchHost + ")");
        elasticSearchPort = context.getString("es_port", "9200");
        int intPort = Integer.parseInt(elasticSearchPort);

        if ((intPort <= 0) || (intPort > 65535)) {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (ckan_port=" + elasticSearchPort + ")"
                    + " -- Must be between 0 and 65535");
        } else {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (ckan_port=" + elasticSearchPort + ")");
        }  // if else
        
        elasticSearchIndex = context.getString("es_index", "index");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (es_index=" + elasticSearchIndex + ")");

        elasticSearchType = context.getString("es_type", "type");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (es_type=" + elasticSearchType + ")");        
        
        super.configure(context);
        
        // Techdebt: allow this sink to work with all the data models
        dataModel = DataModel.DMBYENTITY;
        
        enableLowercase = true;

    } // configure
    
    @Override
    public void start() {
        persistenceBackend = new ElasticSearchBackendImpl(elasticSearchHost, elasticSearchPort);
        LOGGER.debug("[" + this.getName() + "] ElasticSearch persistence backend created");
        super.start();
    } // start
    
    @Override
    void persistBatch(NGSIBatch batch) throws CygnusBadConfiguration, CygnusRuntimeError, CygnusPersistenceError {
        if (batch == null) {
            LOGGER.debug("[" + this.getName() + "] Null batch, nothing to do");
            return;
        } // if

        // Iterate on the destinations
        batch.startIterator();
        
        while (batch.hasNext()) {
            String destination = batch.getNextDestination();
            LOGGER.debug("[" + this.getName() + "] Processing sub-batch regarding the "
                    + destination + " destination");

            // Get the events within the current sub-batch
            ArrayList<NGSIEvent> events = batch.getNextEvents();
            
            // Get the first event, it will give us useful information
            NGSIEvent firstEvent = events.get(0);
            String service = firstEvent.getServiceForData();
            String servicePath = firstEvent.getServicePathForData();

            // Get an aggregator for this entity and initialize it based on the first event
            ElasticSearchAggregator aggregator = new ElasticSearchAggregator();
            aggregator.initialize(firstEvent, this.elasticSearchIndex, this.elasticSearchType);

            for (NGSIEvent event : events) {
                aggregator.aggregate(event);
            } // for

            // Persist the aggregation
            persistAggregation(aggregator, service, servicePath);
            batch.setNextPersisted(true);
        } // while
    } // persistBatch

    @Override
    public void capRecords(NGSIBatch batch, long maxRecords) throws CygnusCappingError {
    } // capRecords

    @Override
    public void expirateRecords(long expirationTime) throws CygnusExpiratingError {
    } // expirateRecords
    
    /**
     * 
     * @param aggregator
     * @throws CygnusPersistenceError
     */
    private void persistAggregation(ElasticSearchAggregator aggregator,String service, String servicePath) throws CygnusPersistenceError {
        String aggregation = aggregator.getAggregation();
        
        if (aggregation.isEmpty()) {
            return;
        } // if
        
        String index = aggregator.getIndex();
        String type = aggregator.getType();
        LOGGER.info("[" + this.getName() + "] Persisting data at NGSIElasticSearchSink. Index: "
                + index + ", Type: " + type + ", Data: " + aggregation.toString());
        
        try {
        	persistenceBackend.persist(aggregation);
        } catch (Exception e) {
            throw new CygnusPersistenceError("-, " + e.getMessage());
        } // try catch
    } // persistAggregation
    
    /**
     * Class for aggregating batches.
     */
    private class ElasticSearchAggregator {
        
        // string containing the data fieldValues
        protected String bulkOperations;

        protected String service;
        protected String servicePathForData;
        protected String servicePathForNaming;
        protected String entityForNaming;
        protected String attributeForNaming;
        protected String index;
        protected String type;
        
        public ElasticSearchAggregator() {
        	bulkOperations = "";
        } // ElasticSearchAggregator
        
        public String getAggregation() {
            return bulkOperations;
        } // getAggregation
        
        public String getIndex() {
            return index;
        } // getIndex
        
        public String getType() {
            return type;
        } // getIndex
        
        public void initialize(NGSIEvent event,String index, String type) throws CygnusBadConfiguration {
            service = event.getServiceForNaming(enableNameMappings);
            servicePathForData = event.getServicePathForData();
            servicePathForNaming = event.getServicePathForNaming(enableGrouping, enableNameMappings);
            entityForNaming = event.getEntityForNaming(enableGrouping, enableNameMappings, enableEncoding);
            attributeForNaming = event.getAttributeForNaming(enableNameMappings);
            this.index = index;
            this.type = type;
        } // initialize
        
        public void aggregate(NGSIEvent cygnusEvent){
            // get the event headers
            long notifiedRecvTimeTs = cygnusEvent.getRecvTimeTs();

            // get the event body
            ContextElement contextElement = cygnusEvent.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");
            
            // iterate on all this context element attributes, if there are attributes
            ArrayList<ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if
            
            for (ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                String attrValue = contextAttribute.getContextValue(false);
                String attrMetadata = contextAttribute.getContextMetadata();
                
                // check if the attribute value is based on white spaces
                if (attrValue.trim().length() == 0) {
                    continue;
                } // if
                
                // check if the metadata contains a TimeInstant value; use the notified reception time instead
                Long recvTimeTs;

                Long timeInstant = CommonUtils.getTimeInstant(attrMetadata);

                if (timeInstant != null) {
                    recvTimeTs = timeInstant;
                } else {
                    recvTimeTs = notifiedRecvTimeTs;
                } // if else
                
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
                
                String bulkOperation = "";
                
                Gson gson = new Gson();
                String operation = "{\"index\" : {\"_index\":\""+index+"\","
                		+ "\"type\" : {\"_type\":\""+type+"\"}}";
                String sourceToIndex = gson.toJson(contextAttribute);
                bulkOperation += operation;
                bulkOperation += "\n";
                bulkOperation += sourceToIndex;
                bulkOperation += "\n";
                bulkOperations += bulkOperation;
            } // for       	
        }
                
    } // MongoDBAggregator

}
