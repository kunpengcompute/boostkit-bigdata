/*
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

package io.prestosql.plugin.hive.functions;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.function.FunctionHandleResolver;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.plugin.hive.functions.FunctionRegistry.addFunction;
import static io.prestosql.plugin.hive.functions.FunctionRegistry.getCurrentFunctionNames;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.initializationError;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.invalidParatemers;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.missParatemers;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    private static final Logger log = Logger.get(HiveFunctionNamespaceManagerFactory.class);
    private static final String FUNCTION_PROPERTIES_FILE_PATH = format("etc%sfunction-namespace", File.separatorChar);
    private static final String EXTERNAL_FUNCTIONS_DIR = "external-functions.dir";

    private ClassLoader classLoader;
    private final FunctionHandleResolver functionHandleResolver;

    public static final String NAME = "hive-functions";

    public HiveFunctionNamespaceManagerFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.functionHandleResolver = new HiveFunctionHandleResolver();
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionHandleResolver getHandleResolver()
    {
        return functionHandleResolver;
    }

    @Override
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext functionNamespaceManagerContext)
    {
        requireNonNull(config, "config is null");

        if (!config.containsKey(EXTERNAL_FUNCTIONS_DIR)) {
            throw missParatemers(format("%s.properties", catalogName), EXTERNAL_FUNCTIONS_DIR);
        }
        this.classLoader = getClassLoader(catalogName);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            config.remove(EXTERNAL_FUNCTIONS_DIR);
            for (String key : config.keySet()) {
                try {
                    if (!getCurrentFunctionNames().contains(key)) {
                        addFunction(key, classLoader.loadClass(config.get(key)));
                    }
                    else {
                        log.warn("Function %s alredy exists.", config.get(key));
                    }
                }
                catch (ClassNotFoundException e) {
                    log.warn("Invalid parameter %s or class %s not Found. %s", key, config.get(key), e.getMessage());
                }
            }

            Bootstrap app = new Bootstrap(
                    new HiveFunctionModule(catalogName, classLoader, functionNamespaceManagerContext.getTypeManager().get()));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(FunctionNamespaceManager.class);
        }
        catch (PrestoException e) {
            throw e;
        }
    }

    private ClassLoader getClassLoader(String catalogName)
    {
        String configFile = System.getProperty("user.dir") + File.separatorChar + FUNCTION_PROPERTIES_FILE_PATH + File.separatorChar + catalogName + ".properties";
        isInSecureDir(configFile);
        File file = new File(configFile);
        List<URL> urls = new ArrayList<>();
        if (file.isFile() && file.length() != 0) {
            try {
                Map<String, String> config = loadPropertiesFrom(configFile);
                isInSecureDir(config.get(EXTERNAL_FUNCTIONS_DIR));
                urls = getURLs(config.get(EXTERNAL_FUNCTIONS_DIR), urls);
            }
            catch (IOException e) {
                throw initializationError(file.getName(), e);
            }
        }
        return new URLClassLoader(urls.toArray(new URL[urls.size()]), this.classLoader);
    }

    private void isInSecureDir(String configFile)
    {
        String filePath;
        try {
            String normalizePath = Normalizer.normalize(configFile, Normalizer.Form.NFKC);
            filePath = new File(normalizePath).getCanonicalPath();
        }
        catch (IOException | IllegalArgumentException e) {
            throw invalidParatemers(configFile, e);
        }
        File file = new File(filePath);
        if (!file.exists()) {
            throw invalidParatemers(filePath);
        }
    }

    private List<URL> getURLs(String externalFunctionsDir, List<URL> urls)
    {
        File dir = new File(externalFunctionsDir);
        String dirName = dir.getName();
        if (!dir.exists() || !dir.isDirectory()) {
            log.debug("%s doesn't exist or is not a directory.", dirName);
            return urls;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            log.debug("%s is empty.", dirName);
            return urls;
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw initializationError(file.getName(), e);
            }
        }
        return urls;
    }
}
