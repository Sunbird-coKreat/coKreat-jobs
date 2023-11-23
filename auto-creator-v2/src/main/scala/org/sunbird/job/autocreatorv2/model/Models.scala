package org.sunbird.job.autocreatorv2.model

case class ExtDataConfig(keyspace: String, table: String, primaryKey:List[String], propsMapping: Map[String, AnyRef])

case class ObjectParent(identifier: String, parents: List[Map[String, String]])

class ObjectData(val identifier: String, val objectType: String, val metadata: Map[String, AnyRef], val extData: Option[Map[String, AnyRef]] = None, val hierarchy: Option[Map[String, AnyRef]] = None)