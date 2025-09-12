package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JsonHelper
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class JsonHelper {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // Configure ObjectMapper (optional, but good practice)
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // Pretty printing
        objectMapper.configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false); // Handle unknown properties gracefully
    }

    /**
     * toJson
     *
     * @param object object
     * @return String
     */
    public static <T> String toJson(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new JsonHelperException("Error serializing object to JSON", e);
        }
    }

    /**
     * fromJson
     *
     * @param json json
     * @param valueType valueType
     * @return T
     */
    public static <T> T fromJson(String json, Class<T> valueType) {
        try {
            return objectMapper.readValue(json, valueType);
        } catch (IOException e) {
            throw new JsonHelperException("Error deserializing JSON to object", e);
        }
    }

    /**
     * writeToFile
     *
     * @param object object
     * @param file file
     */
    public static <T> void writeToFile(T object, java.io.File file) {
        try {
            objectMapper.writeValue(file, object);
        } catch (IOException e) {
            throw new JsonHelperException("Error writing object to file", e);
        }
    }

    /**
     * readFromFile
     *
     * @param file file
     * @param valueType valueType
     * @return T
     */
    public static <T> T readFromFile(java.io.File file, Class<T> valueType) {
        try {
            return objectMapper.readValue(file, valueType);
        } catch (IOException e) {
            throw new JsonHelperException("Error reading object from file", e);
        }
    }

    public static String updateJsonString(String jsonStr, String jarPath) throws IOException {
        JsonNode rootNode = objectMapper.readTree(jsonStr);

        if (rootNode.has("udf_so")) {
            String originalValue = rootNode.get("udf_so").asText();
            String newValue = jarPath + originalValue;
            ((ObjectNode) rootNode).put("udf_so", newValue);
        }

        if (rootNode.has("key_so")) {
            String originalValue = rootNode.get("key_so").asText();
            if (!originalValue.isEmpty()) {
                String newValue = jarPath + originalValue;
                ((ObjectNode) rootNode).put("key_so", newValue);
            }
        }

        ((ObjectNode) rootNode).put("hash_path", jarPath);

        return objectMapper.writeValueAsString(rootNode);
    }

    public static class JsonHelperException extends RuntimeException {
        public JsonHelperException(String message, Throwable cause) {
            super(message, cause);
        }
    }


    // Example usage (in your other classes):
    public static void main(String[] args) throws IOException {
        Address address = new Address("123 Main St", "Anytown");
        Person person = new Person("Alice", 30, address, new String[]{"reading", "hiking", "coding"});

        String json = JsonHelper.toJson(person);
        System.out.println("JSON: " + json);

        Person deserializedPerson = JsonHelper.fromJson(json, Person.class);
        System.out.println("Deserialized Person: " + deserializedPerson);

        JsonHelper.writeToFile(person, new java.io.File("person.json"));
        Person personFromFile = JsonHelper.readFromFile(new java.io.File("person.json"), Person.class);
        System.out.println("Person from file: " + personFromFile);
    }

    /**
     * Address
     *
     * @since 2025-04-27
     */
    public static class Address {
        private String street;
        private String city;

        public Address() {
        } // No-args constructor is essential for deserialization

        public Address(String street, String city) {
            this.street = street;
            this.city = city;
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "Address{"
                    + "street='" + street + '\''
                    + ", city='" + city + '\''
                    + '}';
        }
    }

    /**
     * Person
     *
     * @since 2025-04-27
     */
    public static class Person {
        private String name;
        private int age;
        private Address address;
        private String[] hobbies;

        public Person() {
        } // No-args constructor is essential for deserialization

        public Person(String name, int age, Address address, String[] hobbies) {
            this.name = name;
            this.age = age;
            this.address = address;
            this.hobbies = hobbies;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public String[] getHobbies() {
            return hobbies;
        }

        public void setHobbies(String[] hobbies) {
            this.hobbies = hobbies;
        }


        @Override
        public String toString() {
            return "Person{"
                    + "name='" + name + '\''
                    + ", age=" + age
                    + ", address=" + address
                    + ", hobbies=" + Arrays.toString(hobbies)
                    + '}';
        }
    }
}
