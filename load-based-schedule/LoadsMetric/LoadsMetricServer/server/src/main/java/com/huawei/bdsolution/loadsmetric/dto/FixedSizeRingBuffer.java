package com.huawei.bdsolution.loadsmetric.dto;

public class FixedSizeRingBuffer<T> {
    private final int capacity;
    private Object[] buffer;
    private int head = 0;
    private int tail = 0;
    private int count = 0;

    public FixedSizeRingBuffer(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than 0");
        }
        this.capacity = size;
        this.buffer = new Object[size];
    }

    public synchronized void add(T item) {
        if (count == capacity) {
            head = (head + 1) % capacity;
            count--;
        }
        buffer[tail] = item;
        tail = (tail + 1) % capacity;
        count++;
    }

    public synchronized int getSize() {
        return count;
    }

    public synchronized T get(int index) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException();
        }
        return (T) buffer[(head + index) % capacity];
    }

}
