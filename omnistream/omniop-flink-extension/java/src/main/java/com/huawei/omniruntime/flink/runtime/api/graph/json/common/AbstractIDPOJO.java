/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.util.AbstractID;

import java.util.Objects;

/**
 * AbstractIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class AbstractIDPOJO {
    private long upperPart;
    private long lowerPart;

    public AbstractIDPOJO() {
        // Default constructor
    }

    public AbstractIDPOJO(AbstractID abstractID) {
        this.upperPart = abstractID.getUpperPart();
        this.lowerPart = abstractID.getLowerPart();
    }

    public AbstractIDPOJO(long upperPart, long lowerPart) {
        this.upperPart = upperPart;
        this.lowerPart = lowerPart;
    }

    public long getUpperPart() {
        return upperPart;
    }

    public void setUpperPart(long upperPart) {
        this.upperPart = upperPart;
    }

    public long getLowerPart() {
        return lowerPart;
    }

    public void setLowerPart(long lowerPart) {
        this.lowerPart = lowerPart;
    }

    @Override
    public String toString() {
        return "AbstractIDPOJO{"
                + "upperPart=" + upperPart
                + ", lowerPart=" + lowerPart
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof AbstractIDPOJO, "o is not AbstractIDPOJO");
        AbstractIDPOJO that = (AbstractIDPOJO) o;
        return upperPart == that.upperPart && lowerPart == that.lowerPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upperPart, lowerPart);
    }
}

