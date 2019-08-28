package com.opencore.kafka.topictool

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