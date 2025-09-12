package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.Optional;

/**
 * ResourceIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class ResourceIDPOJO {
    private String resourceId;
    private String metadata;

    // Default constructor
    public ResourceIDPOJO() {
    }

    public ResourceIDPOJO(Optional<ResourceID> resourceID) {
        if (resourceID.isPresent()) {
            this.resourceId = resourceID.get().getResourceIdString();
            this.metadata = resourceID.get().getMetadata();
        } else {
            this.resourceId = "NONE";
            this.metadata = "NONE";
        }
    }

    // Full constructor
    public ResourceIDPOJO(String resourceId, String metadata) {
        this.resourceId = resourceId;
        this.metadata = metadata;
    }

    // Getter for resourceId
    public String getResourceId() {
        return resourceId;
    }

    // Setter for resourceId
    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    // Getter for metadata
    public String getMetadata() {
        return metadata;
    }

    // Setter for metadata
    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    // toString method
    @Override
    public String toString() {
        return "ResourceIDPOJO{"
                + "resourceId='" + resourceId + '\''
                + ", metadata='" + metadata + '\''
                + '}';
    }
}
