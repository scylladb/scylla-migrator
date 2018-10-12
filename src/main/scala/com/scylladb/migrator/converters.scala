package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.datastax.spark.connector.types.{
  CustomDriverConverter,
  NullableTypeConverter,
  PrimitiveColumnType,
  TypeConverter
}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.runtime.universe.TypeTag

case object AnotherCustomUUIDConverter extends NullableTypeConverter[UUID] {
  def targetTypeTag = implicitly[TypeTag[UUID]]
  def convertPF = {
    case x: UUID   => x
    case x: String => UUID.fromString(x)
    case x: UTF8String =>
      UUID.fromString(new String(x.getBytes, StandardCharsets.UTF_8))
  }
}

case object CustomTimeUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "timeuuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(AnotherCustomUUIDConverter)
}

case object CustomUUIDType extends PrimitiveColumnType[UUID] {
  def scalaTypeTag = implicitly[TypeTag[UUID]]
  def cqlTypeName = "uuid"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(AnotherCustomUUIDConverter)
}

object CustomUUIDConverter extends CustomDriverConverter {
  import org.apache.spark.sql.{types => catalystTypes}
  import com.datastax.driver.core.DataType
  import com.datastax.spark.connector.types.ColumnType

  override val fromDriverRowExtension
    : PartialFunction[DataType, ColumnType[_]] = {
    case dataType if dataType.getName == DataType.timeuuid().getName =>
      CustomTimeUUIDType
    case dataType if dataType.getName == DataType.uuid().getName =>
      CustomUUIDType
  }

  override val catalystDataType
    : PartialFunction[ColumnType[_], catalystTypes.DataType] = {
    case CustomTimeUUIDType => catalystTypes.StringType
    case CustomUUIDType     => catalystTypes.StringType
  }
}
