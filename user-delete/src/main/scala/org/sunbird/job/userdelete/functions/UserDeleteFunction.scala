package org.sunbird.job.userdelete.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.userdelete.domain.Event
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.UserDeleteConfig
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class UserDeleteFunction(config: UserDeleteConfig, httpUtil: HttpUtil)
                        (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[UserDeleteFunction])
  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    // TODO: Check if object already exists. If exists, add validation based on pkgVersion
    println(event.action);
    println(event.userId);
    println("test-vk");

    if (event.isValid) {
      println("valid");
      //logger.info("Processing event for user delete operation having identifier : " + event.userId)
      //logger.debug("event edata : " + event.eData)
    }
  }
}
