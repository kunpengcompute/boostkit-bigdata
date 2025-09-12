/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.utils;

import com.huawei.omniruntime.flink.runtime.metrics.OmniDescriptiveStatisticsHistogram;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSizeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSimpleCounter;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSumCounter;
import com.huawei.omniruntime.flink.runtime.metrics.OmniTimeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniInternalOperatorIOMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskIOMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskMetricGroup;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ViewUpdater;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for creating and managing Omni metrics. This class provides methods to create various types of Omni
 * metrics, register them with the MetricQueryService, and manage their lifecycle.
 *
 * @since 2025-04-16
 */
public class OmniMetricHelper {
    /**
     * Creates an OmniSimpleCounter instance with the specified parameters.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return An instance of OmniSimpleCounter.
     */
    public static OmniSimpleCounter createOmniSimpleCounter(long omniTaskMetricRef, String scope, String identifier) {
        long nativeRef = createNativeSimpleCounter(omniTaskMetricRef, scope, identifier);
        return new OmniSimpleCounter(nativeRef);
    }

    /**
     * Creates an OmniSumCounter instance with the specified parameters.
     *
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return An instance of OmniSumCounter.
     */
    public static OmniSumCounter createOmniSumCounter(String scope, String identifier) {
        return new OmniSumCounter(0);
    }

    /**
     * Creates an OmniTimeGauge instance with the specified parameters.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return An instance of OmniTimeGauge.
     */
    public static OmniTimeGauge createOmniTimeGauge(long omniTaskMetricRef, String scope, String identifier) {
        long nativeRef = createNativeTimeGauge(omniTaskMetricRef, scope, identifier);
        return new OmniTimeGauge(nativeRef);
    }

    /**
     * Creates an OmniSizeGauge instance with the specified parameters.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return An instance of OmniSizeGauge.
     */
    public static OmniSizeGauge createSizeGauge(long omniTaskMetricRef, String scope, String identifier) {
        long nativeRef = createNativeSizeGauge(omniTaskMetricRef, scope, identifier);
        return new OmniSizeGauge(nativeRef);
    }

    /**
     * Creates an OmniDescriptiveStatisticsHistogram instance with the specified parameters.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param windowSize The size of the histogram window.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return An instance of OmniDescriptiveStatisticsHistogram.
     */
    public static OmniDescriptiveStatisticsHistogram createOmniOmniDescriptiveStatisticsHistogram
    (long omniTaskMetricRef, int windowSize, String scope, String identifier) {
        long nativeRef = createNativeOmniDescriptiveStatisticsHistogram(omniTaskMetricRef, windowSize, scope,
                identifier);
        return new OmniDescriptiveStatisticsHistogram(nativeRef, windowSize);
    }

    /**
     * Creates a native simple counter.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return A long representing the native reference to the simple counter.
     */
    public static native long createNativeSimpleCounter(long omniTaskMetricRef, String scope, String identifier);

    /**
     * Creates a native time gauge.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return A long representing the native reference to the time gauge.
     */
    public static native long createNativeTimeGauge(long omniTaskMetricRef, String scope, String identifier);

    /**
     * Creates a native size gauge.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return A long representing the native reference to the size gauge.
     */
    public static native long createNativeSizeGauge(long omniTaskMetricRef, String scope, String identifier);

    /**
     * Creates a native descriptive statistics histogram.
     *
     * @param omniTaskMetricRef The reference to the OmniTaskMetric.
     * @param windowSize The size of the histogram window.
     * @param scope The scope of the metric.
     * @param identifier The identifier of the metric.
     * @return A long representing the native reference to the descriptive statistics histogram.
     */
    public static native long createNativeOmniDescriptiveStatisticsHistogram(long omniTaskMetricRef, int windowSize,
            String scope, String identifier);

    /**
     * Registers Omni metrics.
     *
     * @param metrics The TaskMetricGroup to register the metrics with.
     * @param nativeRefTaskMetricGroupRef The reference to the native task metric group.
     * @return An instance of OmniTaskMetricGroup.
     */
    public static OmniTaskMetricGroup registerOmniMetrics(TaskMetricGroup metrics, long nativeRefTaskMetricGroupRef) {
        OmniTaskMetricGroup omniTaskMetricGroup = new OmniTaskMetricGroup();
        OmniTaskIOMetricGroup omniTaskIOMetricGroup = registerTaskIOMetrics(metrics, nativeRefTaskMetricGroupRef);
        List<OmniInternalOperatorIOMetricGroup> omniInternalOperatorIOMetricGroups =
                registerInternalOperatorMetric(metrics, nativeRefTaskMetricGroupRef);
        for (OmniInternalOperatorIOMetricGroup omniInternalOperatorIOMetricGroup : omniInternalOperatorIOMetricGroups) {
            omniTaskIOMetricGroup.reuseRecordsInputCounter(omniInternalOperatorIOMetricGroup.getNumRecordsInCounter());
            omniTaskIOMetricGroup.reuseRecordsOutputCounter
                    (omniInternalOperatorIOMetricGroup.getNumRecordsOutCounter());
        }
        omniTaskMetricGroup.setOmniTaskIOMetricGroup(omniTaskIOMetricGroup);
        for (OmniInternalOperatorIOMetricGroup omniInternalOperatorIOMetricGroup : omniInternalOperatorIOMetricGroups) {
            omniTaskMetricGroup.addOperator(omniInternalOperatorIOMetricGroup.getMetricGroupName(),
                    omniInternalOperatorIOMetricGroup);
        }
        return omniTaskMetricGroup;
    }

    /**
     * Gets the field name of a metric in a container object.
     *
     * @param containerObject The object containing the metric.
     * @param metric The metric to find.
     * @return The name of the field representing the metric, or null if not found.
     */
    public static Optional<String> getMetricFieldName(Object containerObject, Metric metric) {
        try {
            Field[] fields = containerObject.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.get(containerObject) == metric) {
                    return Optional.of(field.getName());
                }
            }
        } catch (IllegalAccessException e) {
            throw new GeneralRuntimeException("Failed to access field", e);
        }
        return Optional.empty(); // Return null if no matching field is found
    }

    /**
     * Gets a map of metrics from a ProxyMetricGroup.
     *
     * @param proxyMetricGroup The ProxyMetricGroup to extract metrics from.
     * @param clazz The class of the ProxyMetricGroup.
     * @return A map of metric names to Metric objects.
     */
    public static Map<String, Metric> getMetricMapFromProxyMetricGroup(ProxyMetricGroup proxyMetricGroup,
            Class clazz) {
        Map<String, Metric> metricsMap = new HashMap<>();
        try {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object obj = field.get(proxyMetricGroup);
                if (obj instanceof Metric) {
                    metricsMap.put(field.getName(), (Metric) obj);
                }
            }
        } catch (IllegalAccessException e) {
            throw new GeneralRuntimeException("Failed to access field", e);
        }
        return metricsMap;
    }

    /**
     * Gets the MetricRegistryImpl from a TaskMetricGroup.
     *
     * @param taskMetricGroup The TaskMetricGroup to extract the MetricRegistryImpl from.
     * @return The MetricRegistryImpl instance.
     */
    public static MetricRegistryImpl getMetricRegistry(TaskMetricGroup taskMetricGroup) {
        MetricRegistryImpl metricRegistry = null;
        try {
            Field field = AbstractMetricGroup.class.getDeclaredField("registry");
            field.setAccessible(true);
            Object obj = field.get(taskMetricGroup);
            if (obj instanceof MetricRegistryImpl) {
                metricRegistry = (MetricRegistryImpl) obj;
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException("Failed to access field", e);
        }
        return metricRegistry;
    }

    /**
     * Gets the ViewUpdater from a TaskMetricGroup.
     *
     * @param taskMetricGroup The TaskMetricGroup to extract the ViewUpdater from.
     * @return The ViewUpdater instance.
     */
    public static ViewUpdater getViewUpdater(TaskMetricGroup taskMetricGroup) {
        return doGetViewUpdater(getMetricRegistry(taskMetricGroup));
    }

    /**
     * Gets the ViewUpdater from a MetricRegistryImpl.
     *
     * @param metricRegistry The MetricRegistryImpl to extract the ViewUpdater from.
     * @return The ViewUpdater instance.
     */
    public static ViewUpdater doGetViewUpdater(MetricRegistryImpl metricRegistry) {
        ViewUpdater viewUpdater ;
        try {
            Field field = metricRegistry.getClass().getDeclaredField("viewUpdater");
            field.setAccessible(true);
            Object obj = field.get(metricRegistry);
            if (obj instanceof ViewUpdater) {
                viewUpdater = (ViewUpdater) obj;
            } else {
                throw new GeneralRuntimeException("not a ViewUpdater");
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return viewUpdater;
    }

    /**
     * Gets the MetricQueryService from a MetricRegistryImpl.
     *
     * @param metricRegistry The MetricRegistryImpl to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static MetricQueryService getMetricQueryService(MetricRegistryImpl metricRegistry) {
        MetricQueryService metricQueryService = null;
        try {
            Field field = metricRegistry.getClass().getDeclaredField("queryService");
            field.setAccessible(true);
            Object obj = field.get(metricRegistry);
            if (obj instanceof MetricQueryService) {
                metricQueryService = (MetricQueryService) obj;
            } else {
                throw new GeneralRuntimeException("not a MetricQueryService");
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return metricQueryService;
    }

    /**
     * Gets the MetricQueryService from a TaskMetricGroup.
     *
     * @param metricGroup The TaskMetricGroup to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> getGaugesFromQueryService(TaskMetricGroup metricGroup) {
        return doGetGaugesFromQueryService(getMetricQueryService(getMetricRegistry(metricGroup)));
    }

    /**
     * Gets the MetricQueryService from a MetricQueryService.
     *
     * @param metricQueryService The MetricQueryService to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> doGetGaugesFromQueryService
    (MetricQueryService metricQueryService) {
        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = null;
        try {
            Field field = metricQueryService.getClass().getDeclaredField("gauges");
            field.setAccessible(true);
            Object obj = field.get(metricQueryService);
            gauges = (Map<Gauge<?>, Tuple2<QueryScopeInfo, String>>) obj;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return gauges;
    }

    /**
     * Gets the MetricQueryService from a TaskMetricGroup.
     *
     * @param metricGroup The TaskMetricGroup to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Counter, Tuple2<QueryScopeInfo, String>> getCountersFromQueryService
    (TaskMetricGroup metricGroup) {
        return doGetCountersFromQueryService(getMetricQueryService(getMetricRegistry(metricGroup)));
    }

    /**
     * Gets the MetricQueryService from a MetricQueryService.
     *
     * @param metricQueryService The MetricQueryService to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Counter, Tuple2<QueryScopeInfo, String>> doGetCountersFromQueryService
    (MetricQueryService metricQueryService) {
        Map<Counter, Tuple2<QueryScopeInfo, String>> counters ;
        try {
            Field field = metricQueryService.getClass().getDeclaredField("counters");
            field.setAccessible(true);
            counters = (Map<Counter, Tuple2<QueryScopeInfo, String>>) field.get(metricQueryService);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return counters;
    }

    /**
     * Gets the MetricQueryService from a TaskMetricGroup.
     *
     * @param metricGroup The TaskMetricGroup to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Meter, Tuple2<QueryScopeInfo, String>> getMetersFromQueryService(TaskMetricGroup metricGroup) {
        return doGetMetersFromQueryService(getMetricQueryService(getMetricRegistry(metricGroup)));
    }

    /**
     * Gets the MetricQueryService from a MetricQueryService.
     *
     * @param metricQueryService The MetricQueryService to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Meter, Tuple2<QueryScopeInfo, String>> doGetMetersFromQueryService
    (MetricQueryService metricQueryService) {
        Map<Meter, Tuple2<QueryScopeInfo, String>> meters = null;
        try {
            Field field = metricQueryService.getClass().getDeclaredField("meters");
            field.setAccessible(true);
            meters = (Map<Meter, Tuple2<QueryScopeInfo, String>>) field.get(metricQueryService);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return meters;
    }

    /**
     * Gets the MetricQueryService from a TaskMetricGroup.
     *
     * @param metricGroup The TaskMetricGroup to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Histogram, Tuple2<QueryScopeInfo, String>> getHistogramsFromQueryService
    (TaskMetricGroup metricGroup) {
        return doGetHistogramsFromQueryService(getMetricQueryService(getMetricRegistry(metricGroup)));
    }

    /**
     * Gets the MetricQueryService from a MetricQueryService.
     *
     * @param metricQueryService The MetricQueryService to extract the MetricQueryService from.
     * @return The MetricQueryService instance.
     */
    public static Map<Histogram, Tuple2<QueryScopeInfo, String>> doGetHistogramsFromQueryService
    (MetricQueryService metricQueryService) {
        Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = null;
        try {
            Field field = metricQueryService.getClass().getDeclaredField("histograms");
            field.setAccessible(true);
            histograms = (Map<Histogram, Tuple2<QueryScopeInfo, String>>) field.get(metricQueryService);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return histograms;
    }

    /**
     * Gets the InternalOperatorMetricGroup from a TaskMetricGroup.
     *
     * @param taskMetricGroup The TaskMetricGroup to extract the InternalOperatorMetricGroup from.
     * @return A map of operator names to InternalOperatorMetricGroup instances.
     */
    public static Map<String, InternalOperatorMetricGroup> getInternalOperatorMetricGroup
    (TaskMetricGroup taskMetricGroup) {
        Map<String, InternalOperatorMetricGroup> operators = null;
        try {
            Field field = taskMetricGroup.getClass().getDeclaredField("operators");
            field.setAccessible(true);
            operators = (Map<String, InternalOperatorMetricGroup>) field.get(taskMetricGroup);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
        return operators;
    }

    /**
     * Registers the TaskIOMetricGroup with the specified parameters.
     *
     * @param metrics The TaskMetricGroup to register the metrics with.
     * @param nativeRefTaskMetricGroupRef The reference to the native task metric group.
     * @return An instance of OmniTaskIOMetricGroup.
     */
    public static OmniTaskIOMetricGroup registerTaskIOMetrics(TaskMetricGroup metrics,
            long nativeRefTaskMetricGroupRef) {
        TaskIOMetricGroup taskIOMetricGroup = metrics.getIOMetricGroup();
        Map<String, Metric> originalMetricMap = getMetricMapFromProxyMetricGroup(taskIOMetricGroup,
                TaskIOMetricGroup.class);
        originalMetricMap.remove("numMailsProcessed");
        waitMetricRegisteredInMetricQueryService(metrics, originalMetricMap);
        OmniTaskIOMetricGroup omniTaskIoMetricGroup = new OmniTaskIOMetricGroup(nativeRefTaskMetricGroupRef);
        updateOmniMetricsOnMetricQueryService(metrics, omniTaskIoMetricGroup, originalMetricMap,
                omniTaskIoMetricGroup.getRegisteredMetrics());
        return omniTaskIoMetricGroup;
    }

    /**
     * Registers the InternalOperatorIOMetricGroup with the specified parameters.
     *
     * @param metrics The TaskMetricGroup to register the metrics with.
     * @param nativeRefTaskMetricGroupRef The reference to the native task metric group.
     * @return A list of OmniInternalOperatorIOMetricGroup instances.
     */
    public static List<OmniInternalOperatorIOMetricGroup> registerInternalOperatorMetric(TaskMetricGroup metrics,
            long nativeRefTaskMetricGroupRef) {
        List<OmniInternalOperatorIOMetricGroup> omniInternalOperatorIOMetricGroups = new ArrayList<>();
        Map<String, InternalOperatorMetricGroup> getOperatorMetricGroup = getInternalOperatorMetricGroup(metrics);
        for (Map.Entry<String, InternalOperatorMetricGroup> entry : getOperatorMetricGroup.entrySet()) {
            String operatorName = entry.getKey();
            InternalOperatorMetricGroup internalOperatorMetricGroup = entry.getValue();
            InternalOperatorIOMetricGroup internalOperatorIOMetricGroup =
                    internalOperatorMetricGroup.getIOMetricGroup();
            OmniInternalOperatorIOMetricGroup omniInternalOperatorIOMetricGroup =
                    new OmniInternalOperatorIOMetricGroup(nativeRefTaskMetricGroupRef, operatorName);
            omniInternalOperatorIOMetricGroups.add(omniInternalOperatorIOMetricGroup);
            Map<String, Metric> originalMetricsMap = getMetricMapFromProxyMetricGroup(internalOperatorIOMetricGroup,
                    InternalOperatorIOMetricGroup.class);
            Map<String, Metric> omniRegisteredMetrics = omniInternalOperatorIOMetricGroup.getRegisteredMetrics();
            waitMetricRegisteredInMetricQueryService(metrics, originalMetricsMap);
            updateOmniMetricsOnMetricQueryService(metrics, omniInternalOperatorIOMetricGroup, originalMetricsMap,
                    omniRegisteredMetrics);
        }
        return omniInternalOperatorIOMetricGroups;
    }

    /**
     * Waits for the metrics to be registered in the MetricQueryService.
     *
     * @param metrics The TaskMetricGroup to check.
     * @param originalMetricMap The map of original metrics.
     */
    public static void waitMetricRegisteredInMetricQueryService(TaskMetricGroup metrics,
            Map<String, Metric> originalMetricMap) {
        while (!checkIfMetricRegisteredInMetricQueryService(metrics, originalMetricMap)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new GeneralRuntimeException(e);
            }
        }
    }

    /**
     * Checks if the metrics are registered in the MetricQueryService.
     *
     * @param metrics The TaskMetricGroup to check.
     * @param originalMetricMap The map of original metrics.
     * @return True if all metrics are registered, false otherwise.
     */
    public static boolean checkIfMetricRegisteredInMetricQueryService(TaskMetricGroup metrics,
            Map<String, Metric> originalMetricMap) {
        boolean completelyRegistered = true;
        Map<Counter, Tuple2<QueryScopeInfo, String>> counterMap = getCountersFromQueryService(metrics);
        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gaugeMap = getGaugesFromQueryService(metrics);
        Map<Meter, Tuple2<QueryScopeInfo, String>> meterMap = getMetersFromQueryService(metrics);
        Map<Histogram, Tuple2<QueryScopeInfo, String>> histogramMap = getHistogramsFromQueryService(metrics);
        for (Map.Entry<String, Metric> entry : originalMetricMap.entrySet()) {
            Metric metric = entry.getValue();
            if (metric instanceof Counter) {
                if (!counterMap.containsKey((Counter) metric)) {
                    completelyRegistered = false;
                }
            } else if (metric instanceof Gauge) {
                if (!gaugeMap.containsKey((Gauge<?>) metric)) {
                    completelyRegistered = false;
                }
            } else if (metric instanceof Meter) {
                if (!meterMap.containsKey((Meter) metric)) {
                    completelyRegistered = false;
                }
            } else if (metric instanceof Histogram) {
                if (!histogramMap.containsKey((Histogram) metric)) {
                    completelyRegistered = false;
                }
            } else {
                continue;
            }
        }
        return completelyRegistered;
    }

    /**
     * Updates the Omni metrics on the MetricQueryService.
     *
     * @param metrics The TaskMetricGroup to update.
     * @param omniMetricContainer The container for the Omni metrics.
     * @param originalMetricMap The map of original metrics.
     * @param omniRegisteredMetrics The map of registered Omni metrics.
     */
    public static void updateOmniMetricsOnMetricQueryService(TaskMetricGroup metrics, Object omniMetricContainer,
            Map<String, Metric> originalMetricMap, Map<String, Metric> omniRegisteredMetrics) {
        Map<Counter, Tuple2<QueryScopeInfo, String>> counterMap = getCountersFromQueryService(metrics);
        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gaugeMap = getGaugesFromQueryService(metrics);
        Map<Meter, Tuple2<QueryScopeInfo, String>> meterMap = getMetersFromQueryService(metrics);
        Map<Histogram, Tuple2<QueryScopeInfo, String>> histogramMap = getHistogramsFromQueryService(metrics);
        ViewUpdater viewUpdater = getViewUpdater(metrics);
        for (Map.Entry<String, Metric> entry : omniRegisteredMetrics.entrySet()) {
            Metric metric = entry.getValue();
            Optional<String> fieldOption = getMetricFieldName(omniMetricContainer, metric);
            if (fieldOption.isPresent()) {
                String fieldName = fieldOption.get();
                Metric originalMetric = originalMetricMap.get(fieldName);
                if (metric instanceof Counter) {
                    Tuple2<QueryScopeInfo, String> queryScopeInfo = counterMap.get(originalMetric);
                    counterMap.remove(originalMetric);
                    counterMap.put((Counter) metric, queryScopeInfo);
                } else if (metric instanceof Gauge) {
                    Tuple2<QueryScopeInfo, String> queryScopeInfo = gaugeMap.get(originalMetric);
                    gaugeMap.remove(originalMetric);
                    gaugeMap.put((Gauge<?>) metric, queryScopeInfo);
                } else if (metric instanceof Meter) {
                    Tuple2<QueryScopeInfo, String> queryScopeInfo = meterMap.get(originalMetric);
                    meterMap.remove(originalMetric);
                    meterMap.put((Meter) metric, queryScopeInfo);
                } else if (metric instanceof Histogram) {
                    Tuple2<QueryScopeInfo, String> queryScopeInfo = histogramMap.get(originalMetric);
                    histogramMap.remove(originalMetric);
                    histogramMap.put((Histogram) metric, queryScopeInfo);
                } else {
                    continue;
                }
                //
                if (metric instanceof View && originalMetric instanceof View) {
                    // add to viewUpdater
                    viewUpdater.notifyOfAddedView((View) metric);
                    viewUpdater.notifyOfRemovedView((View) originalMetric);
                }
            }
        }
    }
}
