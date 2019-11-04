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
package com.opencore.kafka

import kafka.admin.ReassignPartitionsCommand
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

class ScalaInterface(val adminClientParam: AdminClient, val zkConnectString: String) {
  val zkClient: KafkaZkClient = KafkaZkClient(zkConnectString, JaasUtils.isZkSecurityEnabled, 30000, 30000, Integer.MAX_VALUE, Time.SYSTEM)
  var adminZkClientOpt: Option[AdminClient] = Some(adminClientParam)
  var NoThrottle: ReassignPartitionsCommand.Throttle = ReassignPartitionsCommand.Throttle(-1, -1)

  def execute(plan: String): Unit = {
    ReassignPartitionsCommand.executeAssignment(zkClient, adminZkClientOpt, plan, NoThrottle, 10000L)
  }
}