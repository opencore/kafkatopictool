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
package com.opencore.kafka.topictool.serialization;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

public class Murmur2Deserializer implements Deserializer {

  @Override
  public void configure(Map map, boolean b) {

  }

  @Override
  public Integer deserialize(String s, byte[] bytes) {
    return Utils.murmur2(bytes);
  }

  @Override
  public void close() {

  }
}
