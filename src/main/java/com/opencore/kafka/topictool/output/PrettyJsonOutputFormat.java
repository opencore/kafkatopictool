/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.opencore.kafka.topictool.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PrettyJsonOutputFormat extends JsonOutputFormat {

  public static final String FORMAT_NAME = "prettyjson";

  @Override
  public String formatName() {
    return PrettyJsonOutputFormat.FORMAT_NAME;
  }

  @Override
  protected Gson getGson() {
    return new GsonBuilder().setPrettyPrinting().create();
  }
}
