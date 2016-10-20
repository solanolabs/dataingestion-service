package com.ge.predix.solsvc.dataingestion.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.DataFormatException;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.ge.predix.entity.timeseries.datapoints.ingestionrequest.Body;
import com.ge.predix.entity.timeseries.datapoints.ingestionrequest.DatapointsIngestion;
import com.ge.predix.solsvc.dataingestion.boot.DataingestionServiceApplication;
import com.ge.predix.solsvc.dataingestion.handler.TimeSeriesDataIngestionHandler;
import com.ge.predix.solsvc.dataingestion.service.type.JSONData;
import com.ge.predix.solsvc.restclient.impl.RestClient;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesRestConfig;
import com.neovisionaries.ws.client.WebSocketException;

/**
 * 
 * @author predix -
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = DataingestionServiceApplication.class)
@WebAppConfiguration
@IntegrationTest({ "server.port=0" })
public class PredixCSVDataingestionServiceTestHarness {
	private static final Logger logger = LoggerFactory.getLogger(PredixCSVDataingestionServiceTestHarness.class);

	@Value("${local.server.port}")
	private int localServerPort;

	@Autowired
	private TimeSeriesDataIngestionHandler timeseriesDataIngestionHandler;

	@Autowired
	private RestClient restClient;

	@Autowired
	private TimeseriesRestConfig timeseriesRestConfig;

	/**
	 * @throws Exception
	 *             -
	 */
	@Before
	public void setUp() throws Exception {
		//
	}

	/**
	 * @throws IOException
	 *             -
	 * @throws WebSocketException
	 *             -
	 * @throws DataFormatException
	 *             -
	 */
	@SuppressWarnings("nls")
	@Test
	public void csvIngestionTest() throws IOException, WebSocketException, DataFormatException {
		FileInputStream stream = null;
		try {
			File file = new File("src/test/resources/timeseries-ingest.xls");
			//File file = new File("src/test/resources/dataset.xls");
			stream = new FileInputStream(file);
			DatapointsIngestion datapointsIngestion = getDatapointsIngestion(stream);
			this.timeseriesDataIngestionHandler.ingestTimeseriesData(datapointsIngestion);
			// assertThat(response, startsWith("You successfully posted data"));
		} finally {
			if (stream != null)
				stream.close();
		}

	}

	/**
	 * @param stream
	 * @return -
	 * @throws DataFormatException
	 */
	@SuppressWarnings("nls")
	private DatapointsIngestion getDatapointsIngestion(FileInputStream stream) throws DataFormatException {
		List<String> workSheets = new ArrayList<>();
		workSheets.add("timeseries-ingest"); //$NON-NLS-1$

		SpreadSheetParser parser = new SpreadSheetParser();
		Map<String, String[][]> content = parser.parseInputFile(stream, workSheets);

		List<Header> headers = this.restClient.getSecureTokenForClientId();
		this.restClient.addZoneToHeaders(headers, this.timeseriesRestConfig.getZoneId());
		headers.add(new BasicHeader("Origin", "http://localhost"));

		DatapointsIngestion dpIngestion = getDatapointsIngestion(content);

		return dpIngestion;
	}

	@SuppressWarnings("nls")
	private DatapointsIngestion getDatapointsIngestion(Map<String, String[][]> content) {
		String[][] rows = content.get("timeseries-ingest");
		DatapointsIngestion dpIngestion = new DatapointsIngestion();
		dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));

		int i = 0;
		List<Body> bodies = new ArrayList<Body>();
		HashMap<String, Body> bodyMap = new HashMap<String, Body>();
		String[] headerRow = null;
		DateTimeFormatter df = null;
		SimpleDateFormat sdf = null;
		String timeFormat = null;
		for (String[] row : rows) {
			if (i++ == 0) {
				headerRow = row;
				String firstCol = row[0];
				if (firstCol.startsWith("Date")) {
					timeFormat = firstCol.substring(firstCol.indexOf("(") + 1, firstCol.indexOf(")"));
				}
				df = DateTimeFormatter.ofPattern(timeFormat);
				sdf = new SimpleDateFormat(timeFormat);
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				for (int j = 1; j < row.length; j++) {
					Body body = new Body();
					bodies.add(body);
					String name = row[j++];
					body.setName(name);
					bodyMap.put(name, body);
				}
			} else {
				String time = row[0];
				String epoch;
				try {
					if (timeFormat.startsWith("S")) {
						// assume epoch
						String decimal = new BigDecimal(time).toPlainString();
						Date date = new Date(Long.parseLong(decimal));
						epoch = Long.toString(date.getTime());
					} else {
						Date date = sdf.parse(time);
						epoch = Long.toString(date.getTime());
					}
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
				for (int j = 1; j < row.length; j++) {
					String value = row[j];
					String quality = row[j];
					@SuppressWarnings("null")
					Body body = bodyMap.get(headerRow[j++]);
					String[] datapoint = new String[3];
					body.getDatapoints().add(datapoint);
					datapoint[0] = epoch;
					datapoint[1] = value;
					datapoint[2] = quality;
				}
			}
		}
		logger.trace("UUID :" + dpIngestion.getMessageId() + " injection" + dpIngestion.toString()); //$NON-NLS-1$ //$NON-NLS-2$
		dpIngestion.setBody(bodies);

		return dpIngestion;
	}

	/**
	 * @return -
	 */
	@SuppressWarnings("nls")
	List<JSONData> generateMockDataMap_RT() {
		String machineControllerId = "1";
		List<JSONData> list = new ArrayList<JSONData>();
		JSONData data = new JSONData();
		data.setName("Compressor-2015:CompressionRatio");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(2.5, 3.0) - 1) * 65535.0 / 9.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:DischargePressure");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(0.0, 23.0) * 65535.0) / 100.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:SuctionPressure");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(0.0, 0.21) * 65535.0) / 100.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:MaximumPressure");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(22.0, 26.0) * 65535.0) / 100.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:MinimumPressure");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue(0.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:Temperature");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(65.0, 80.0) * 65535.0) / 200.0);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		data = new JSONData();
		data.setName("Compressor-2015:Velocity");
		data.setTimestamp(getCurrentTimestamp());
		data.setValue((generateRandomUsageValue(0.0, 0.1) * 65535.0) / 0.5);
		data.setDatatype("DOUBLE");
		data.setRegister("");
		data.setUnit(machineControllerId);
		list.add(data);

		return list;
	}

	private Timestamp getCurrentTimestamp() {
		java.util.Date date = new java.util.Date();
		Timestamp ts = new Timestamp(date.getTime());
		return ts;
	}

	private static double generateRandomUsageValue(double low, double high) {
		return low + Math.random() * (high - low);
	}

}
