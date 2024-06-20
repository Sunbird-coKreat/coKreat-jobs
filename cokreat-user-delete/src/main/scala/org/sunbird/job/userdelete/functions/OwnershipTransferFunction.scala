package org.sunbird.job.userdelete.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.ServerException
import org.sunbird.job.userdelete.domain.Event
import org.sunbird.job.task.UserDeleteConfig
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class OwnershipTransferFunction (config: UserDeleteConfig, httpUtil: HttpUtil)
                                (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                                 stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config){

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[OwnershipTransferFunction])

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
    logger.info("OwnershipTransferFunction :: processElement :: Event :: " + event)


    if (event.isValid(logger)) {
      logger.info("Processing event for ownership transfer operation from user : " + event.eData.get("fromUserProfile") + " to user : " + event.eData.get("toUserProfile"))

      val requestUrl = s"${config.programServiceBaseUrl}/program/v1/user/transfer"
      logger.debug("Ownership Transfer :: requestUrl: " + requestUrl)
      val reqMap: Map[String, AnyRef] = Map("request"-> event.map)
      logger.debug("Ownership Transfer :: serialize :: reqMap: " + JSONUtil.serialize(reqMap))
      val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap))

      logger.info("Ownership Transfer api response :: " + httpResponse.body)
      val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body);
      if (httpResponse.status == 200) {
        val responseCode = response.getOrElse("responseCode", 0).asInstanceOf[String]
        if (responseCode == "OK") {
          logger.info("Ownership Transfer Event is triggered :: Success")
        } else {
          logger.error("Ownership Transfer Event is failed")
        }
      } else if (httpResponse.status != 500) {
        val result = response.getOrElse("result", Map()).asInstanceOf[Map[String, AnyRef]]
        logger.error("OwnershipTransfer :: failed with an error" + result);
      }
      else {
        throw new ServerException("UserDelete:: ERR_API_CALL", "Invalid Response received while deleting user for : " + getErrorDetails(httpResponse))
      }
    }
    context.output(config.ownershipTransferOutTag, event)
  }

  private def getErrorDetails(httpResponse: HTTPResponse): String = {
    logger.info("UserDelete:: getErrorDetails:: httpResponse.body:: " + httpResponse.body)
    val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
    if (null != response) " | Response Code :" + httpResponse.status + " | Result : " + response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] + " | Error Message : " + response.getOrElse("params", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    else " | Null Response Received."
  }
}
