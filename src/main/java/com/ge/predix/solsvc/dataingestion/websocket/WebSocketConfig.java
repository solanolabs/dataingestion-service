/*
 * Copyright (c) 2015 General Electric Company. All rights reserved.
 *
 * The copyright to the computer software herein is the property of
 * General Electric Company. The software may be used and/or copied only
 * with the written permission of General Electric Company or in accordance
 * with the terms and conditions stipulated in the agreement/contract
 * under which the software has been supplied.
 */
 
package com.ge.predix.solsvc.dataingestion.websocket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.ge.predix.solsvc.websocket.config.IWebSocketConfig;

/**
 * 
 * @author predix -
 */
@Component
public class WebSocketConfig implements IWebSocketConfig
{

    @Value("${predix.oauth.proxyHost:}")
    private String wsProxyHost;

    @Value("${predix.oauth.proxyPort:}")
    private String wsProxyPort;

    @Value("${predix.websocket.uri}")
    private String wsUri;

    @Value("${predix.websocket.zoneid:null}")
    private String zoneId;

    @Value("${predix.websocket.pool.maxIdle:}")
    private int    wsMaxIdle;

    @Value("${predix.websocket.pool.maxActive:}")
    private int    wsMaxActive;

    @Value("${predix.websocket.pool.maxWait}")
    private int    wsMaxWait;

    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsMaxIdle()
     */
    @Override
    public int getWsMaxIdle()
    {
        return this.wsMaxIdle;
    }



    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsMaxActive()
     */
    @Override
    public int getWsMaxActive()
    {
        return this.wsMaxActive;
    }


    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsMaxWait()
     */
    @Override
    public int getWsMaxWait()
    {
        return this.wsMaxWait;
    }

    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsProxyHost()
     */
    @Override
    public String getWsProxyHost()
    {
        return this.wsProxyHost;
    }


    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsProxyPort()
     */
    @Override
    public String getWsProxyPort()
    {
        return this.wsProxyPort;
    }


    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getWsUri()
     */
    @Override
    public String getWsUri()
    {
        return this.wsUri;
    }


    /*
     * (non-Javadoc)
     * @see com.ge.predix.solsvc.websocket.config.IWebSocketConfig#getZoneId()
     */
    @Override
    public String getZoneId()
    {
        return this.zoneId;
    }



    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @SuppressWarnings("nls")
    @Override
    public String toString()
    {
        return "WebSocketConfig" + this.hashCode() + " [wsProxyHost=" + this.wsProxyHost + ", wsProxyPort=" + this.wsProxyPort + ", wsUri=" + this.wsUri 
                + ", zoneId=" + this.zoneId + ", wsMaxIdle=" + this.wsMaxIdle + ", wsMaxActive=" + this.wsMaxActive + ", wsMaxWait=" 
                + this.wsMaxWait + "]"; 
    }

    
}