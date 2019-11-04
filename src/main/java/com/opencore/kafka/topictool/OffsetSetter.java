/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 *
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
package com.opencore.kafka.topictool;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;

public class OffsetSetter {
    Map<String, Properties> clusterPropertiesMap;
    Map<String, KafkaConsumer<Byte[], Byte[]>> consumerMap;
    private Logger logger = LoggerFactory.getLogger(OffsetSetter.class);

    public OffsetSetter(Map<String, Properties> clusterPropertiesMap) {
        this.clusterPropertiesMap = clusterPropertiesMap;
        this.consumerMap = new HashMap<>();

        for (String clusterName : clusterPropertiesMap.keySet()) {
            Properties clusterProps = clusterPropertiesMap.get(clusterName);
            clusterProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
            clusterProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
            consumerMap.put(clusterName, new KafkaConsumer<Byte[], Byte[]>(clusterProps));
        }
    }

    public void setOffsets(File offsetsFile, List<String> clusters, boolean useDate) {
        // Load offset data from csv file
        List<CSVRecord> offsetRecords;
        logger.debug("Starting to parse offsets from file " + offsetsFile.toString());
        try {
            FileReader offsetsReader = new FileReader(offsetsFile);
            CSVParser csvParser = new CSVParser(offsetsReader, CSVFormat.DEFAULT);
            offsetRecords = csvParser.getRecords();
            logger.debug("Got " + offsetRecords.size() + " records.");
        } catch (IOException e) {
            logger.error("Error occured when loading the offsets file: " + e.getMessage());
            return;
        }

        Map<String, Map<String, Map<TopicPartition, OffsetAndMetadata>>> clusterToOffsets;
        clusterToOffsets = useDate ? getClusterOffsets(offsetRecords) : getClusterOffsetsFromDates(offsetRecords);

        for (String cluster : clusterToOffsets.keySet()) {
            Properties groupProperties = clusterPropertiesMap.get(cluster);
            for (String consumerGroup : clusterToOffsets.get(cluster).keySet()) {
                // We need to create a consumer per group to set the offsets
                groupProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
                groupProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
                groupProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
                KafkaConsumer<Byte[], Byte[]> groupConsumer = new KafkaConsumer<>(groupProperties);
                groupConsumer.commitSync(clusterToOffsets.get(cluster).get(consumerGroup));
            }
        }
    }

    private Map<String, Map<String, Map<TopicPartition, OffsetAndMetadata>>> getClusterOffsets(List<CSVRecord> records) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> groupToValuesMap = new HashMap<>();
        logger.debug("Parsing value from csv file as offset.");
        for (CSVRecord record : records) {
            String topic = record.get(0);
            int partition = Integer.valueOf(record.get(1));
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            String consumerGroup = record.get(2);
            Long offset = Long.valueOf(record.get(3));

            if (!groupToValuesMap.containsKey(consumerGroup)) {
                groupToValuesMap.put(consumerGroup, new HashMap<TopicPartition, OffsetAndMetadata>());
            }
            groupToValuesMap.get(consumerGroup).put(topicPartition, new OffsetAndMetadata(offset));
            logger.trace("Got record: " + consumerGroup + ":" + topic + "-" + partition + ":" + offset);
        }

        // Copy per cluster
        Map<String, Map<String, Map<TopicPartition, OffsetAndMetadata>>> result = new HashMap<>();
        for (String cluster : consumerMap.keySet()) {
            result.put(cluster, groupToValuesMap);
        }
        return result;
    }

    private Map<String, Map<String, Map<TopicPartition, OffsetAndMetadata>>> getClusterOffsetsFromDates(List<CSVRecord> records) {
        Map<String, Map<TopicPartition, Long>> groupToPartitionToDatesMap = new HashMap<>();
        logger.debug("Parsing value from csv file as date.");
        String[] datePatterns = new String[] {"dd/MM/yyyy'T'HH:mm:ss"};
        for (CSVRecord record : records) {
            String topic = record.get(0);
            int partition = Integer.valueOf(record.get(1));
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            String consumerGroup = record.get(2);
            Long date = -1L;
            try {
                Date parsedDate = DateUtils.parseDateStrictly(record.get(3), datePatterns);
                date = parsedDate.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            if (!groupToPartitionToDatesMap.containsKey(consumerGroup)) {
                groupToPartitionToDatesMap.put(consumerGroup, new HashMap<>());
            }
            groupToPartitionToDatesMap.get(consumerGroup).put(topicPartition, date);
            logger.trace("Got record: " + consumerGroup + ":" + topic + "-" + partition + ":" + date);
        }

        // Copy per cluster
        Map<String, Map<String, Map<TopicPartition, OffsetAndMetadata>>> result = new HashMap<>();
        for (String cluster : consumerMap.keySet()) {
            KafkaConsumer<Byte[], Byte[]> consumer = consumerMap.get(cluster);
            result.put(cluster, new HashMap<>());
            for (String consumerGroup : groupToPartitionToDatesMap.keySet()) {
                logger.debug("Looking up offsets for dates on cluster " + cluster + " and consumergroup " + consumerGroup);
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimestamps = consumer.offsetsForTimes(groupToPartitionToDatesMap.get(consumerGroup));

                logger.trace("Converting offsets to necessary format.");
                // necessary transformation due to API requirements
                Map<TopicPartition, OffsetAndMetadata> convertedOffsetsForTimestamps = new HashMap<>();
                for (TopicPartition topicPartition : offsetsForTimestamps.keySet()) {
                    convertedOffsetsForTimestamps.put(topicPartition, new OffsetAndMetadata(offsetsForTimestamps.get(topicPartition).offset()));
                }
                result.get(cluster).put(consumerGroup, convertedOffsetsForTimestamps);
            }
        }
        return result;
    }
}
