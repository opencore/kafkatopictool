package com.opencore.kafka.topictool.output;

import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;

public interface OutputFormatService {
  String formatName();
  String format(List<NewTopic> topicList);
}
