package com.huawei.boostkit.omnidata.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class NdpConnectorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NdpConnectorUtils.class);

    public static Set<String> getIpAddress() {
        Set<String> ipSet = new HashSet<>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (!netInterface.isLoopback() && !netInterface.isVirtual() && netInterface.isUp()) {
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip instanceof Inet4Address) {
                            ipSet.add(ip.getHostAddress());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("getIpAddress exception:", e);
        }
        return ipSet;
    }

    public static String getNdpEnable() {
        return System.getenv("NDP_PLUGIN_ENABLE") == null ? "false" : System.getenv("NDP_PLUGIN_ENABLE");
    }

}
