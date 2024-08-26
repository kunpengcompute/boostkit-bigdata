package com.huawei.bdsolution.loadsmetric.service.impl;

import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.service.NodeHeartbeatService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class NodeHeartbeatServiceImpl implements NodeHeartbeatService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeHeartbeatServiceImpl.class);

    private final Set<String> allNodeHosts = new HashSet<>();

    @Value("${client.lost.time:2000}")
    private long nodeLostTime;

    private final Map<String, Long> allNodeHeartbeatTimes = new ConcurrentHashMap<>();

    private static final ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    /**
     * initAllNodeHosts from workers config
     */
    @PostConstruct
    public void initAllNodeHosts() {
        try(InputStream inputStream = Files.newInputStream(Paths.get(System.getProperty("workersConfig")));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            allNodeHosts.addAll(reader.lines().collect(Collectors.toList()));
            LOG.info("allNodeHosts:{}", allNodeHosts);
        }catch (IOException e) {
            throw new RuntimeException("load workersConfig failed for",e);
        }
    }

    /**
     * update node's latest heartbeat time
     *
     * @param loadsRecords LoadsRecords
     */
    public void updateNodeHeartbeatTime(LoadsRecords loadsRecords) {
        try{
            long time = dateFormatThreadLocal.get().parse(loadsRecords.getTimeStamp()).getTime();
            allNodeHeartbeatTimes.put(loadsRecords.getHostName(), time);
        }catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * schedule every client.lost.time to do heartbeat check
     */
    @Scheduled(fixedRateString = "${client.lost.time:2000}", initialDelay = 5000)
    public void reportLostNodes() {
        long currentTime = System.currentTimeMillis();
        for (String nodeHost : allNodeHosts) {
            if (!allNodeHeartbeatTimes.containsKey(nodeHost)) {
                LOG.warn("node {} never report loadsRecords", nodeHost);
                continue;
            }
            Long lastHeartbeatTime = allNodeHeartbeatTimes.get(nodeHost);
            if (currentTime - lastHeartbeatTime > nodeLostTime) {
                LOG.warn("node {} hasn't report loadsRecords since {}", nodeHost, new Date(lastHeartbeatTime));
            }
        }
    }

    /**
     * get all node hosts
     *
     * @return all node hosts
     */
    public Set<String> getAllNodeHosts() {
        return allNodeHosts;
    }
}
