package com.ge.predix.solsvc.dataingestion.handler;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.ge.predix.solsvc.bootstrap.ams.common.AssetRestConfig;
import com.ge.predix.solsvc.restclient.impl.RestClient;


/**
 * 
 * @author 212421693
 *
 */
public abstract class BaseFactory {
	@SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(BaseFactory.class);
	
	
	/**
	 * 
	 */
	@Value("${predix.timeseries.retry.count}")
	protected int retryCount;
	
	/**
	 * 
	 */
	@Autowired
	protected RestClient restClient;

	/**
	 * 
	 */
	@Autowired
	protected AssetRestConfig assetRestConfig;

	

}
