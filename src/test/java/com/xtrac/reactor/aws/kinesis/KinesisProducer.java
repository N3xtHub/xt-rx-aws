/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xtrac.reactor.aws.kinesis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.xtrac.Config;

public class KinesisProducer {

	final static Logger log = LoggerFactory.getLogger(KinesisProducer.class);

	public static final void main(String[] args) throws Exception {

		Properties configProps = readConfig();
		Config config = new Config(configProps);

		ClientConfiguration clientConfiguration = new ClientConfiguration();

		if (config.getProxyHost() != null && !config.getProxyHost().equals("")) {
			clientConfiguration.setProxyHost(config.getProxyHost());
			clientConfiguration.setProxyPort(config.getProxyPort());
		}

		AmazonKinesisClient client = new AmazonKinesisClient(clientConfiguration);
		Regions region = Regions.fromName(config.getRegionName());
		client.setRegion(Region.getRegion(region));

		for (int i = 0; i < 5; i++) {
			PutRecordResult prr = client.putRecord(config.getStream(),
					ByteBuffer.wrap(("{\"foo\":\"bar\"}".getBytes())), "test");
			log.info(prr.getSequenceNumber());

		}
		Thread.sleep(1);

	}

	private static Properties readConfig() throws FileNotFoundException, IOException {
		String propFilePath = System.getenv("CONFIG_PATH");

		log.info("config path: " + propFilePath);
		System.err.println("prop file path is " + propFilePath);
		if (propFilePath == null) {
			throw new RuntimeException(
					"CONFIG_PATH environment variable not set - cannot read configuration properties");
		}

		File file = new File(propFilePath);
		FileInputStream fileInput = new FileInputStream(file);
		Properties properties = new Properties();
		properties.load(fileInput);
		fileInput.close();

		return properties;
	}
}
