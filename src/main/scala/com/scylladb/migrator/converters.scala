package com.scylladb.migrator

import java.net.InetAddress
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

case object CustomInetType extends PrimitiveColumnType[InetAddress] {
  def scalaTypeTag = implicitly[TypeTag[InetAddress]]
  def cqlTypeName = "inet"
  def converterToCassandra =
    new TypeConverter.OptionToNullConverter(CustomInetAddressConverter)
}

case object CustomInetAddressConverter extends NullableTypeConverter[InetAddress] {
  def targetTypeTag = implicitly[TypeTag[InetAddress]]
  def convertPF = {
    case x: InetAddress => x
    case x: String      => InetAddress.getByName(x)
    case x: UTF8String  => InetAddress.getByName(new String(x.getBytes, StandardCharsets.UTF_8))
  }
}

object CustomUUIDConverter extends CustomDriverConverter {
  import org.apache.spark.sql.{ types => catalystTypes }
  import com.datastax.driver.core.DataType
  import com.datastax.spark.connector.types.ColumnType

  override val fromDriverRowExtension: PartialFunction[DataType, ColumnType[_]] = {
    case dataType if dataType.getName == DataType.timeuuid().getName =>
      CustomTimeUUIDType
    case dataType if dataType.getName == DataType.uuid().getName =>
      CustomUUIDType
    case dataType if dataType.getName == DataType.inet().getName =>
      CustomInetType
  }

  override val catalystDataType: PartialFunction[ColumnType[_], catalystTypes.DataType] = {
    case CustomTimeUUIDType => catalystTypes.StringType
    case CustomUUIDType     => catalystTypes.StringType
    case CustomInetType     => catalystTypes.StringType
  }
}
