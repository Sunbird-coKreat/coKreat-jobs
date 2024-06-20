package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.userdelete.domain.Event

import scala.collection.JavaConverters._

class UserDeleteConfig(override val config: Config) extends BaseJobConfig(config, "cokreat-user-delete") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.user_delete_topic")
  val ownershipTransferKafkaInputTopic: String = config.getString("kafka.input.ownership_transfer_topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "user-delete-consumer"
  val ownershipTransferEventConsumer = "ownership-transfer-consumer"
  val UserDeleteFunction = "user-delete"
  val OwnershipTransferFunction = "ownership-transfer"
  val programServiceBaseUrl: String = config.getString("program-service-baseUrl")

  // Out Tags
  val ownershipTransferOutTag: OutputTag[Event] = OutputTag[Event]("ownership-transfer")

  val configVersion = "1.0"

  def getConfig() = config
}
