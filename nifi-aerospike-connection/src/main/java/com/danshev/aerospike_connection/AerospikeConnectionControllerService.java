/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.danshev.aerospike_connection;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Tags({ "aerospike"})
@CapabilityDescription("Implementation of the Aerospike Controller Service.")
public class AerospikeConnectionControllerService extends AbstractControllerService implements AerospikeConnectionService {

    private static final Logger log = LoggerFactory.getLogger(AerospikeConnectionControllerService.class);

    public static final PropertyDescriptor AEROSPIKE_HOSTS = new PropertyDescriptor
        .Builder().name("AEROSPIKE_HOSTS")
        .displayName("Aerospike Hosts List")
        .description("A pipe-separated list of 'host.name', port values for each of the Aerospike servers (e.g., 'a.host.name',3000|'another.host.name', 3000")
        .expressionLanguageSupported(false)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor AEROSPIKE_USERNAME = new PropertyDescriptor
        .Builder().name("AEROSPIKE_USERNAME")
        .displayName("Aerospike connection username")
        .description("If necessary, the username for the Aerospike connection")
        .expressionLanguageSupported(false)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor AEROSPIKE_PASSWORD = new PropertyDescriptor
        .Builder().name("AEROSPIKE_PASSWORD")
        .displayName("Aerospike connection password")
        .description("If necessary, the password for the Aerospike connection")
        .expressionLanguageSupported(false)
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static AerospikeClient aerospikeClient = null;
    private static ClientPolicy policy = new ClientPolicy();

    private static String aerospike_host_string = "";
    private static String aerospike_username = "";
    private static String aerospike_password = "";

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AEROSPIKE_HOSTS);
        props.add(AEROSPIKE_USERNAME);
        props.add(AEROSPIKE_PASSWORD);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        // Load properties
        aerospike_host_string = context.getProperty(AEROSPIKE_HOSTS).getValue();
        aerospike_username = context.getProperty(AEROSPIKE_USERNAME).getValue();
        aerospike_password = context.getProperty(AEROSPIKE_PASSWORD).getValue();

        // TODO: determine if password is null or not -- if not null then:
        //policy.user = aerospike_username;
        //policy.password = aerospike_password;

        aerospikeClient = getConnection(); // Try the connection
        if (aerospikeClient == null) {
            log.error("Error: Couldn't connect to Aerospike.");
        }
    }

    @OnDisabled
    public void shutdown() {

    }

    @Override
    public AerospikeClient getConnection() throws ProcessException {
        try {
            if (aerospikeClient == null) {

                String[] hostStringsArray = aerospike_host_string.split("\\|[ ]*");
                Integer hostsCount = hostStringsArray.length;

                Host[] hosts = new Host[hostsCount];

                for (int i = 0; i < hostStringsArray.length; i++) {
                    String[] hostPortArray = hostStringsArray[i].split(",[ ]*");
                    hosts[i] = new Host(hostPortArray[0], Integer.parseInt(hostPortArray[1]));
                }

                aerospikeClient = new AerospikeClient(policy, hosts);
            }
        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            e.printStackTrace();
        }

        return aerospikeClient;
    }
}