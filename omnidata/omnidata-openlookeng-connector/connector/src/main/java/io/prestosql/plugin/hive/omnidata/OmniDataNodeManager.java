/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive.omnidata;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptors;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.spnego.KerberosConfig;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.Normalizer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.spi.HostAddress.fromParts;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class OmniDataNodeManager
{
    private static final Logger log = Logger.get(OmniDataNodeManager.class);
    public static final String CONFIG_PROPERTY = "config";

    @GuardedBy("this")
    private Map<String, OmniDataNodeStatus> allNodes = new ConcurrentHashMap<>();

    private final ScheduledExecutorService nodeStateUpdateExecutor;

    private KerberosConfig kerberosConfig;
    private HttpClientConfig httpClientConfig;
    private DiscoveryClientConfig discoveryClientConfig;
    private boolean httpsRequired;
    private AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public OmniDataNodeManager()
    {
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("omnidata-node-state-poller-%s"));
    }

    private boolean initializeConfiguration()
    {
        String configFilePath = System.getProperty(CONFIG_PROPERTY);
        if (configFilePath == null || configFilePath.isEmpty()) {
            log.error("System property %s does not exist.", CONFIG_PROPERTY);
            return false;
        }

        String filePath;
        try {
            String normalizePath = Normalizer.normalize(configFilePath, Normalizer.Form.NFKC);
            filePath = new File(normalizePath).getCanonicalPath();
        }
        catch (IOException | IllegalArgumentException exception) {
            log.error("config file path [%s] is invalid, exception %s", configFilePath, exception.getMessage());
            return false;
        }
        File file = new File(filePath);
        if (!file.exists()) {
            log.error("config file [%s] does not exist.", filePath);
            return false;
        }

        Map<String, String> properties;
        try {
            properties = loadPropertiesFrom(configFilePath);
        }
        catch (IOException e) {
            log.error("Fail to load config file, Check your configuration.");
            return false;
        }

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        this.kerberosConfig = configurationFactory.build(KerberosConfig.class);
        this.httpClientConfig = configurationFactory.build(HttpClientConfig.class);
        this.discoveryClientConfig = configurationFactory.build(DiscoveryClientConfig.class);
        CommunicationConfig communicationConfig = configurationFactory.build(CommunicationConfig.class);
        this.httpsRequired = communicationConfig.isHttpsRequired();
        return true;
    }

    public void startPollingNodeStates()
    {
        if (started.getAndSet(true)) {
            return;
        }

        if (!initializeConfiguration()) {
            return;
        }
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshNodes();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of omnidata nodes");
            }
        }, 10, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (!started.get()) {
            return;
        }

        nodeStateUpdateExecutor.shutdownNow();
    }

    private synchronized void refreshNodes()
    {
        allNodes.clear();

        Set<ServiceDescriptor> services = getServices().getServiceDescriptors().stream().collect(toImmutableSet());
        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service, httpsRequired);
            String localHdfsIpAddress = service.getProperties().get("local.hdfs.server.address");
            String grpcPort = service.getProperties().get("grpc.server.port");
            String runningTaskNumber = service.getProperties().get("runningTaskNumber");
            String maxTaskNumber = service.getProperties().get("maxTaskNumber");
            if (uri.getHost() != null && localHdfsIpAddress != null) {
                try {
                    OmniDataNodeStatus nodeStatus = new OmniDataNodeStatus(fromParts(uri.getHost(), Integer.parseInt(grpcPort)).toString(),
                            Integer.parseInt(runningTaskNumber), Integer.parseInt(maxTaskNumber));
                    allNodes.put(localHdfsIpAddress, nodeStatus);
                }
                catch (RuntimeException ignored) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "omnidata node manger receive wrong arguments");
                }
            }
        }
    }

    private ServiceDescriptors getServices()
    {
        try (HttpClient httpClient = new JettyHttpClient("omnidata-node-manager", httpClientConfig, kerberosConfig, ImmutableList.of())) {
            URI uri = discoveryClientConfig.getDiscoveryServiceURI();
            if (uri == null) {
                throw new DiscoveryException("No discovery servers are available");
            }

            String type = "omnidata";
            uri = URI.create(uri + "/v1/service/" + type + "/");

            Request.Builder requestBuilder = Request.Builder.prepareGet()
                    .setUri(uri)
                    .setHeader("User-Agent", System.getProperty("node.id"));

            return httpClient.execute(requestBuilder.build(), new OmniDataNodeManagerResponseHandler<ServiceDescriptors>(type, uri) {
                @Override
                public ServiceDescriptors handle(Request request, Response response)
                {
                    if (response.getStatusCode() != HttpStatus.OK.code()) {
                        throw new DiscoveryException(String.format("Lookup of %s failed with status code %s", type, response.getStatusCode()));
                    }

                    byte[] json;
                    try {
                        json = ByteStreams.toByteArray(response.getInputStream());
                    }
                    catch (IOException e) {
                        throw new DiscoveryException(format("Lookup of %s failed", type), e);
                    }

                    JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec = JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class);
                    ServiceDescriptorsRepresentation serviceDescriptorsRepresentation = serviceDescriptorsCodec.fromJson(json);

                    Duration maxAge = DiscoveryAnnouncementClient.DEFAULT_DELAY;
                    String eTag = response.getHeader(HttpHeaders.ETAG);

                    return new ServiceDescriptors(
                            type,
                            null,
                            serviceDescriptorsRepresentation.getServiceDescriptors(),
                            maxAge,
                            eTag);
                }
            });
        }
    }

    private static URI getHttpUri(ServiceDescriptor descriptor, boolean httpsRequired)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }

    public synchronized Map<String, OmniDataNodeStatus> getAllNodes()
    {
        return allNodes;
    }

    public synchronized OmniDataNodeStatus getNode(HostAddress host)
    {
        return allNodes.get(host);
    }

    private class OmniDataNodeManagerResponseHandler<T>
            implements ResponseHandler<T, DiscoveryException>
    {
        private final String type;
        private final URI uri;

        protected OmniDataNodeManagerResponseHandler(String name, URI uri)
        {
            this.type = name;
            this.uri = uri;
        }

        @Override
        public T handle(Request request, Response response)
        {
            return null;
        }

        @Override
        public final T handleException(Request request, Exception exception)
        {
            if (exception instanceof InterruptedException) {
                throw new DiscoveryException("Lookup" + type + " was interrupted for " + uri);
            }
            if (exception instanceof CancellationException) {
                throw new DiscoveryException("Lookup" + type + " was canceled for " + uri);
            }
            if (exception instanceof DiscoveryException) {
                throw (DiscoveryException) exception;
            }

            throw new DiscoveryException("Lookup" + type + " failed for " + uri, exception);
        }
    }
}
