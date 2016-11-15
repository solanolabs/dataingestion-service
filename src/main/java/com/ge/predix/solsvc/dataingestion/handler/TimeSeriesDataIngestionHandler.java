package com.ge.predix.solsvc.dataingestion.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import javax.annotation.PostConstruct;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.ge.predix.entity.asset.Asset;
import com.ge.predix.entity.asset.AssetTag;
import com.ge.predix.entity.timeseries.datapoints.ingestionrequest.Body;
import com.ge.predix.entity.timeseries.datapoints.ingestionrequest.DatapointsIngestion;
import com.ge.predix.solsvc.dataingestion.api.Constants;
import com.ge.predix.solsvc.dataingestion.service.type.JSONData;
import com.ge.predix.solsvc.dataingestion.websocket.WebSocketConfig;
import com.ge.predix.solsvc.ext.util.JsonMapper;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesRestConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.factories.TimeseriesFactory;
import com.ge.predix.solsvc.websocket.client.WebSocketClient;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;

/**
 * 
 * @author predix -
 */
@Component
public class TimeSeriesDataIngestionHandler extends BaseFactory
{
    private static Logger        log      = LoggerFactory.getLogger(TimeSeriesDataIngestionHandler.class);
    @Autowired
    private TimeseriesFactory    timeSeriesFactory;

    @Autowired
    private AssetDataHandler     assetDataHandler;

    @Autowired
    private WebSocketClient      wsServerClient;

    @Autowired
    private WebSocketConfig      websocketConfig;

    @Autowired
    private JsonMapper           jsonMapper;

    @Autowired
    private TimeseriesRestConfig timeseriesRestConfig;

    private Map<String, Asset>   assetMap = new HashMap<String, Asset>();

    // private WebSocketAdapter adapter;

    /**
     * -
     */
    @SuppressWarnings("nls")
    @PostConstruct
    public void intilizeDataIngestionHandler()
    {

        try
        {
            WebSocketAdapter listener = new WebSocketAdapter()
            {
                private Logger logger = LoggerFactory.getLogger(TimeSeriesDataIngestionHandler.class);

                @Override
                public void onTextMessage(WebSocket wsocket, String message)
                {
                    this.logger.info("Success from Predix Websocket Server message=" + message);
                }
            };
            List<Header> nullHeaders = null;
            this.wsServerClient.init(this.websocketConfig, nullHeaders , listener);

            WebSocketAdapter nullListener = null;
            this.timeSeriesFactory.createConnectionToTimeseriesWebsocket(nullListener);
            log.info("*******************TimeSeriesDataIngestionHandler Initialization complete*********************");
        }
        catch (Throwable e)
        {
            log.error(e.getMessage(), e);
            throw new RuntimeException("unable to complete task", e);
        }
    }

    /**
     * @param data -
     * @param authorization -
     */
    @SuppressWarnings("nls")
    public void handleData(String data, String authorization)
    {
        try
        {
            log.info("Data from Simulator : " + data);
            List<Header> headers = this.restClient.getSecureTokenForClientId();
            this.restClient.addZoneToHeaders(headers, this.timeseriesRestConfig.getZoneId());
            headers.add(new BasicHeader("Origin", "http://localhost"));
            ObjectMapper mapper = new ObjectMapper();
            List<JSONData> list = mapper.readValue(data, new TypeReference<List<JSONData>>()
            {
                //
            });

            DatapointsIngestion dpIngestion = createTimeseriesDataBody(list, authorization);
            ingestTimeseriesData(dpIngestion);

        }
        catch (JsonParseException e)
        {
            throw new RuntimeException(e);
        }
        catch (JsonMappingException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (WebSocketException e)
        {
            throw new RuntimeException(e);
        }
    }

	/**
	 * @param dpIngestion - 
	 * @throws IOException -
	 * @throws WebSocketException -
	 */
	@SuppressWarnings("nls")
	public void ingestTimeseriesData(DatapointsIngestion dpIngestion) throws IOException, WebSocketException {
		String jsonToSend = this.jsonMapper.toJson(dpIngestion);
		log.info("TimeSeries JSON : " + jsonToSend);
		if ( dpIngestion.getBody() != null && dpIngestion.getBody().size() > 0 )
		{
		    this.wsServerClient.postTextWSData(jsonToSend);
		    log.info("Posted Predix Websocket Server using clientInfo=" + this.wsServerClient);

		    this.timeSeriesFactory.postDataToTimeseriesWebsocket(dpIngestion);
		    log.info("Posted Data to Timeseries");
		}
	}

    /**
     * @param json
     * @param i
     * @param asset
     * @param assetTag
     * @return -
     */
    @SuppressWarnings(
    {
            "unchecked", "unused", "nls"
    })
    private DatapointsIngestion createTimeseriesDataBody(JSONData json, Long i, Asset asset, AssetTag assetTag)
    {
        DatapointsIngestion dpIngestion = new DatapointsIngestion();
        dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));

        Body body = new Body();
        body.setName(assetTag.getSourceTagId());

        // attributes
        com.ge.predix.entity.util.map.Map map = new com.ge.predix.entity.util.map.Map();
        map.put("assetId", asset.getAssetId());
        if ( !StringUtils.isEmpty(assetTag.getSourceTagId()) )
        {
            String sourceTagAttribute = assetTag.getSourceTagId().split(":")[1];
            map.put("sourceTagId", sourceTagAttribute);
        }
        body.setAttributes(map);

        // datapoints
        List<Object> datapoint1 = new ArrayList<Object>();
        datapoint1.add(converLocalTimeToUtcTime(json.getTimestamp().getTime()));
        Double convertedValue = getConvertedValue(assetTag.getTagDatasource().getNodeName(),
                Double.parseDouble(json.getValue().toString()));
        datapoint1.add(convertedValue);

        List<Object> datapoints = new ArrayList<Object>();
        datapoints.add(datapoint1);
        body.setDatapoints(datapoints);

        List<Body> bodies = new ArrayList<Body>();
        bodies.add(body);

        dpIngestion.setBody(bodies);

        return dpIngestion;
    }

    @SuppressWarnings(
    {
            "unchecked", "nls"
    })
    private DatapointsIngestion createTimeseriesDataBody(List<JSONData> jsonData, String authorization)
    {
        DatapointsIngestion dpIngestion = new DatapointsIngestion();
        dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));
        List<Body> bodies = new ArrayList<Body>();

        for (JSONData data : jsonData)
        {
            String filter = "attributes.machineControllerId.value";
            String value = "/asset/Bently.Nevada.3500.Rack" + data.getUnit();
            String nodeName = data.getName();
            Asset asset = this.assetMap.get(value);
            if ( asset == null )
            {
                asset = this.assetDataHandler.retrieveAsset(null, filter, value, authorization);
                if ( asset == null )
                {
                    throw new RuntimeException("Error retriving asset for filter=" + filter + "=" + value); //$NON-NLS-1$
                }
                this.assetMap.put(value, asset);
            }
            if ( asset != null )
            {
                LinkedHashMap<String, AssetTag> tags = asset.getAssetTag();
                if ( tags != null )
                {
                    Body body = new Body();

                    AssetTag assetTag = getAssetTag(asset.getAssetTag(), nodeName);
                    body.setName(assetTag.getSourceTagId());
                    // attributes
                    com.ge.predix.entity.util.map.Map map = new com.ge.predix.entity.util.map.Map();
                    map.put("assetId", asset.getAssetId());
                    if ( !StringUtils.isEmpty(assetTag.getSourceTagId()) )
                    {
                        String sourceTagAttribute = assetTag.getSourceTagId().split(":")[1];
                        map.put("sourceTagId", sourceTagAttribute);
                    }
                    body.setAttributes(map);

                    // datapoints
                    List<Object> datapoint1 = new ArrayList<Object>();
                    datapoint1.add(converLocalTimeToUtcTime(data.getTimestamp().getTime()));
                    Double convertedValue = getConvertedValue(assetTag.getTagDatasource().getNodeName(),
                            Double.parseDouble(data.getValue().toString()));
                    datapoint1.add(convertedValue);

                    List<Object> datapoints = new ArrayList<Object>();
                    datapoints.add(datapoint1);
                    body.setDatapoints(datapoints);

                    bodies.add(body);
                }
            }
        }

        dpIngestion.setBody(bodies);

        return dpIngestion;
    }

    /**
     * @param nodeName
     *            -
     * @param value
     *            -
     * @return -
     */
    @SuppressWarnings("nls")
    public Double getConvertedValue(String nodeName, Double value)
    {
        Double convValue = null;
        Pattern pattern = Pattern.compile("^(.*)-[0-9]*:(.*)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(nodeName);

        String nodeType = "";
        if (matcher.matches()) {  // find() ?
                nodeType = matcher.group(1) + "-" + matcher.group(2);
        }

        switch (nodeType.toLowerCase())
        {
            case "compressor-compressionratio":
                convValue = value * 9.0 / 65535.0 + 1;
                break;
            case "compressor-dischargepressure":
                convValue = value * 100.0 / 65535.0;
                break;
            case "compressor-suctionpressure":
                convValue = value * 100.0 / 65535.0;
                break;
            case "compressor-maximumpressure":
                convValue = value * 100.0 / 65535.0;
                break;
            case "compressor-minimumpressure":
                convValue = value * 100.0 / 65535.0;
                break;
            case "compressor-velocity":
                convValue = value * 0.5 / 65535.0;
                break;
            case "compressor-temperature":
                convValue = value * 200.0 / 65535.0;
                break;
            default:
                throw new UnsupportedOperationException("nodeName=" + nodeName + " not found");
        }
        return convValue;
    }

    private long converLocalTimeToUtcTime(long timeSinceLocalEpoch)
    {
        return timeSinceLocalEpoch + getLocalToUtcDelta();
    }

    private long getLocalToUtcDelta()
    {
        Calendar local = Calendar.getInstance();
        local.clear();
        local.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        return local.getTimeInMillis();
    }

    @SuppressWarnings("nls")
    private AssetTag getAssetTag(LinkedHashMap<String, AssetTag> tags, String nodeName)
    {
        AssetTag ret = null;
        if ( tags != null )
        {
            for (Entry<String, AssetTag> entry : tags.entrySet())
            {
                AssetTag assetTag = entry.getValue();
                // TagDatasource dataSource = assetTag.getTagDatasource();
                if ( assetTag != null && !assetTag.getSourceTagId().isEmpty() && nodeName != null
                        && nodeName.toLowerCase().contains(assetTag.getSourceTagId().toLowerCase()) )
                {
                    ret = assetTag;
                    return ret;
                }
            }
        }
        else
        {
            log.warn("2. asset has no assetTags with matching nodeName" + nodeName);
        }
        return ret;
    }
}
