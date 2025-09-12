/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.utils;

import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * ReflectionUtils
 *
 * @since 2025-04-27
 */
public class ReflectionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * retrievePrivateField
     *
     * @param obj obj
     * @param fieldName fieldName
     * @return T
     */
    public static <T> T retrievePrivateField(Object obj, String fieldName) {
        try {
            Class<?> clazz = obj.getClass();
            Field field = null;

            // Search for the field in the class hierarchy
            while (clazz != null) {
                try {
                    field = clazz.getDeclaredField(fieldName);
                    break; // Field found, exit the loop
                } catch (NoSuchFieldException e) {
                    clazz = clazz.getSuperclass(); // Check the superclass
                }
            }

            if (field == null) {
                throw new FlinkRuntimeException("Field '"
                        + fieldName
                        + "' not found in class hierarchy of "
                        + obj.getClass().getName());
            }
            field.setAccessible(true); // Make the field accessible
            return (T) field.get(obj);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            LOG.warn("Error retrieving private field: " + e.getMessage());
            throw new FlinkRuntimeException("Error retrieving private field: " + e.getMessage(), e);
        }
    }

    /**
     * setPrivateField
     *
     * @param obj obj
     * @param fieldName fieldName
     * @param arg arg
     * @param <T> <T>
     */
    public static <T> void setPrivateField(Object obj, String fieldName, Object arg) {
        try {
            Class<?> clazz = obj.getClass();
            Field field = null;

            // Search for the field in the class hierarchy
            while (clazz != null) {
                try {
                    field = clazz.getDeclaredField(fieldName);
                    break; // Field found, exit the loop
                } catch (NoSuchFieldException e) {
                    clazz = clazz.getSuperclass(); // Check the superclass
                }
            }

            if (field == null) {
                throw new FlinkRuntimeException("Field '"
                    + fieldName
                    + "' not found in class hierarchy of "
                    + obj.getClass().getName());
            }

            field.setAccessible(true); // Make the field accessible
            field.set(obj, arg);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            LOG.warn("Error retrieving private field: " + e.getMessage());
            throw new FlinkRuntimeException("Error retrieving private field: " + e.getMessage(), e);
        }
    }

    /**
     * invokeMethod
     *
     * @param obj obj
     * @param methodName methodName
     * @param args args
     * @param parameterTypes parameterTypes
     * @return T
     */
    public static <T> T invokeMethod(Object obj, String methodName, List<Object> args, Class<?>... parameterTypes) {
        try {
            Class<?> clazz = obj.getClass();
            Method method = null;

            // Search for the field in the class hierarchy
            while (clazz != null) {
                try {
                    method = clazz.getDeclaredMethod(methodName, parameterTypes);
                    break; // Field found, exit the loop
                } catch (NoSuchMethodException e) {
                    clazz = clazz.getSuperclass(); // Check the superclass
                }
            }

            if (method == null) {
                throw new FlinkRuntimeException("Method '"
                    + methodName
                    + "' not found in class hierarchy of "
                    + obj.getClass().getName());
            }

            method.setAccessible(true); // Make the field accessible

            // Cast is safe because of the generic type T
            return (T) method.invoke(obj, args.toArray());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            LOG.warn("Error retrieving private field: " + e.getMessage());
            throw new FlinkRuntimeException("Error retrieving private field: " + e.getMessage(), e);
        }
    }

    /**
     * invokeStaticMethod
     *
     * @param className className
     * @param methodName methodName
     * @param args args
     * @param parameterTypes parameterTypes
     * @return T
     */
    public static <T> T invokeStaticMethod(String className, String methodName,
        List<Object> args, Class<?>... parameterTypes) {
        try {
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            Method method = null;

            // Search for the field in the class hierarchy
            while (clazz != null) {
                try {
                    method = clazz.getDeclaredMethod(methodName, parameterTypes);
                    break; // Field found, exit the loop
                } catch (NoSuchMethodException e) {
                    clazz = clazz.getSuperclass(); // Check the superclass
                }
            }

            if (method == null) {
                throw new FlinkRuntimeException("Method '"
                    + methodName
                    + "' not found in class hierarchy of "
                    + className);
            }

            method.setAccessible(true); // Make the field accessible

            return (T) method.invoke(null, args.toArray());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | ClassNotFoundException e) {
            LOG.warn("Error retrieving private field: " + e.getMessage());
            throw new FlinkRuntimeException("Error retrieving private field: " + e.getMessage(), e);
        }
    }

    /**
     * constructInstance
     *
     * @param className className
     * @param args args
     * @param parameterTypes parameterTypes
     * @return T
     */
    public static <T> T constructInstance(String className, List<Object> args, Class<?>... parameterTypes) {
        try {
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            Constructor<?> constructor = clazz.getDeclaredConstructor(parameterTypes);
            constructor.setAccessible(true);
            return (T) constructor.newInstance(args.toArray());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
            | ClassNotFoundException | NoSuchMethodException e) {
            LOG.warn("Error find or access given class constructor: " + e.getMessage());
            throw new FlinkRuntimeException("Error find or access given class constructor: " + e.getMessage(), e);
        } catch (InstantiationException e) {
            LOG.warn("Error instantiate given class: " + e.getMessage());
            throw new FlinkRuntimeException("Error instantiate given class: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        // Example usage:
        class Parent {
            private String parentField = "Parent Value";

            public String getParentField() {
                return parentField;
            }
        }

        class Child extends Parent {
            private int childField = 42;

            public int getChildField() {
                return childField;
            }
        }

        Child child = new Child();

        String parentFieldValue = ReflectionUtils.retrievePrivateField(child, "parentField");
        int childFieldValue = ReflectionUtils.retrievePrivateField(child, "childField");

        System.out.println("Parent Field Value: " + parentFieldValue);  // Output: Parent Value
        System.out.println("Child Field Value: " + childFieldValue);    // Output: 42

        // Example of a non-existent field to show exception handling:
        try {
            String nonExistentField = ReflectionUtils.retrievePrivateField(child, "nonExistentField");
        } catch (RuntimeException e) {
            System.err.println("Caught RuntimeException: " + e.getMessage()); // Correctly catches and logs
        }

        Parent parent = new Parent();
        String parentFieldValue2 = ReflectionUtils.retrievePrivateField(parent, "parentField");
        System.out.println("Parent Field Value (parent instance): " + parentFieldValue2);  // Output: Parent Value
    }
}


