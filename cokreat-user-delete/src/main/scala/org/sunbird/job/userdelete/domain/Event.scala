package org.sunbird.job.userdelete.domain

import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "cokreat-user-delete"
	private val validEventAction = List("delete-user", "ownership-transfer")

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]
	def action: String = readOrDefault[String]("edata.action", "")
	def userId: String = readOrDefault[String]("edata.userId", "")
	def objType: String = readOrDefault[String]("object.type", "")

	def fromUserProfile: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.fromUserProfile", Map.empty[String, AnyRef])
	def toUserProfile: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.toUserProfile", Map.empty[String, AnyRef])

	def isValid(logger: Logger): Boolean = {
		validEventAction.contains(action) && StringUtils.equalsIgnoreCase("User", objType) && (action match {
			case "delete-user" => StringUtils.isNotBlank(userId)
			case "ownership-transfer" => validateFromUserProfile(fromUserProfile, logger) && validateToUserProfile(toUserProfile, logger)
			case _ => false
			})
	}
	def isValid(): Boolean = {
		validEventAction.contains(action) && StringUtils.equalsIgnoreCase("User", objType) && (action match {
			case "delete-user" => StringUtils.isNotBlank(userId)
			case _ => false
		})
	}

	def validateFromUserProfile(data: Map[String, AnyRef], logger: Logger): Boolean = {
		val userId: String = data.getOrElse("userId", "").asInstanceOf[String]
		logger.info("Event :: validateToUserProfile :: userId: " + userId)
		StringUtils.isNotBlank(userId)
	}

  	def validateToUserProfile(data: Map[String, AnyRef], logger: Logger): Boolean = {
    	val userId: String = data.getOrElse("userId", "").asInstanceOf[String]
    	val firstName: String = data.getOrElse("firstName", "").asInstanceOf[String]
    	val lastName: String = data.getOrElse("lastName", "").asInstanceOf[String]
    	val roles: List[String] = data.getOrElse("roles", List()).asInstanceOf[List[String]]
			logger.info("Event :: validateToUserProfile :: userId: " + userId + " :: firstName: " + firstName + " :: lastName: " + lastName + " :: roles: " + roles)
    	StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(firstName) && StringUtils.isNotBlank(lastName) && !roles.isEmpty
  	}
}