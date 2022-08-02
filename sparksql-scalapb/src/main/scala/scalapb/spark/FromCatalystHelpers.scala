package scalapb.spark

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects.{
  Invoke,
  MapObjects,
  NewInstance,
  StaticInvoke
}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, ObjectType, StructType}
import scalapb.GeneratedMessageCompanion
import scalapb.descriptors._
import org.apache.spark.sql.catalyst.expressions.objects.CatalystToExternalMap
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

import scala.collection.immutable

trait FromCatalystHelpers {
  def protoSql: ProtoSQL

  def schemaOptions: SchemaOptions = protoSql.schemaOptions

  def pmessageFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression,
      dataTypeOpt: Option[StructType] = None
  ): Expression = {
    val columnsOpt = dataTypeOpt.map(_.names.toSet)
    val containsColumn: String => Boolean = (dataTypeOpt, columnsOpt) match {
      case (Some(_), Some(columns)) =>
        field => columns.contains(field)
      case _ => _ => true
    }
    val args: immutable.Seq[Expression] =
      if (schemaOptions.isUnpackedPrimitiveWrapper(cmp.scalaDescriptor))
        cmp.scalaDescriptor.fields.map { fd =>
          if (containsColumn(fd.name)) {
            fieldFromCatalyst(
              cmp,
              fd,
              input,
              dataTypeOpt.map(dataType => dataType(fd.name).dataType)
            )
          } else {
            Literal.fromObject(PEmpty, ObjectType(classOf[PValue]))
          }
        }
      else if (
        schemaOptions.sparkTimestamps && cmp.scalaDescriptor.fullName == "google.protobuf.Timestamp"
      ) {
        immutable.Seq(
          StaticInvoke(
            JavaTimestampHelpers.getClass,
            ObjectType(classOf[PValue]),
            "extractSecondsFromMicrosTimestamp",
            input :: Nil
          ),
          StaticInvoke(
            JavaTimestampHelpers.getClass,
            ObjectType(classOf[PValue]),
            "extractNanosFromMicrosTimestamp",
            input :: Nil
          )
        )
      } else {
        val expressions: immutable.Seq[Expression] = cmp.scalaDescriptor.fields.map { fd =>
          if (containsColumn(fd.name)) {
            val newPath = addToPath(input, schemaOptions.columnNaming.fieldName(fd))
            fieldFromCatalyst(
              cmp,
              fd,
              newPath,
              dataTypeOpt.map(dataType => dataType(fd.name).dataType)
            )
          } else {
            Literal.fromObject(PEmpty, ObjectType(classOf[PValue]))
          }
        }
        expressions
      }
    val outputType = ObjectType(classOf[PValue])
    val mapArgs =
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[Map[FieldDescriptor, PValue]]),
        "mkMap",
        Literal.fromObject(cmp) :: CreateArray(args) :: Nil
      )
    If(
      IsNull(input),
      Literal.create(null, outputType),
      NewInstance(classOf[PMessage], mapArgs :: Nil, outputType)
    )
  }

  def fieldFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      input: Expression,
      dataType: Option[DataType] = None
  ): Expression = {
    if (fd.isRepeated && !fd.isMapField) {
      val elementType = dataType.map(_.asInstanceOf[ArrayType].elementType)
      val objs = MapObjects(
        (input: Expression) => singleFieldValueFromCatalyst(cmp, fd, input, elementType),
        input,
        protoSql.singularDataType(fd, elementType)
      )
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[PValue]),
        "mkPRepeated",
        objs :: Nil
      )
    } else if (fd.isRepeated && fd.isMapField) {
      val mapEntryCmp = cmp.messageCompanionForFieldNumber(fd.number)
      val keyDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(1).get
      val valDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(2).get
      val urobjs = MyUnresolvedCatalystToExternalMap(
        input,
        (in: Expression) =>
          singleFieldValueFromCatalyst(
            mapEntryCmp,
            keyDesc,
            in,
            dataType.map(_.asInstanceOf[MapType].keyType)
          ),
        (in: Expression) =>
          singleFieldValueFromCatalyst(
            mapEntryCmp,
            valDesc,
            in,
            dataType.map(_.asInstanceOf[MapType].valueType)
          ),
        ProtoSQL.dataTypeFor(fd, dataType).asInstanceOf[MapType],
        classOf[Vector[(Any, Any)]]
      )
      val objs = MyCatalystToExternalMap(urobjs)
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[PValue]),
        "mkPRepeatedMap",
        Literal.fromObject(mapEntryCmp) ::
          objs :: Nil
      )
    } else if (fd.isOptional)
      If(
        IsNull(input),
        Literal.fromObject(PEmpty, ObjectType(classOf[PValue])),
        singleFieldValueFromCatalyst(cmp, fd, input, dataType)
      )
    else singleFieldValueFromCatalyst(cmp, fd, input, dataType)
  }

  def singleFieldValueFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      input: Expression,
      dataType: Option[DataType] = None
  ): Expression = {
    fd.scalaType match {
      case ScalaType.ByteString =>
        val bs = StaticInvoke(
          classOf[ByteString],
          ObjectType(classOf[ByteString]),
          "copyFrom",
          input :: Nil
        )
        NewInstance(
          classOf[PByteString],
          bs :: Nil,
          ObjectType(classOf[PValue])
        )
      case ScalaType.Enum(_) =>
        val evd = cmp.enumCompanionForFieldNumber(fd.number)
        StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[PValue]),
          "penumFromString",
          Literal.fromObject(evd) :: input :: Nil
        )
      case ScalaType.Message(_) =>
        pmessageFromCatalyst(
          cmp.messageCompanionForFieldNumber(fd.number),
          input,
          dataType.map(_.asInstanceOf[StructType])
        )
      case ScalaType.String =>
        val asString = Invoke(input, "toString", ObjectType(classOf[String]))
        NewInstance(
          classOf[PString],
          asString :: Nil,
          ObjectType(classOf[PValue])
        )
      case ScalaType.Int =>
        NewInstance(classOf[PInt], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Long =>
        NewInstance(classOf[PLong], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Double =>
        NewInstance(classOf[PDouble], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Float =>
        NewInstance(classOf[PFloat], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Boolean =>
        NewInstance(
          classOf[PBoolean],
          input :: Nil,
          ObjectType(classOf[PValue])
        )
    }
  }

  def addToPath(path: Expression, name: String): Expression = {
    UnresolvedExtractValue(path, expressions.Literal(name))
  }
}

case class MyUnresolvedCatalystToExternalMap(
    child: Expression,
    @transient keyFunction: Expression => Expression,
    @transient valueFunction: Expression => Expression,
    mapType: MapType,
    collClass: Class[_]
)

object MyCatalystToExternalMap {
  def apply(u: MyUnresolvedCatalystToExternalMap): CatalystToExternalMap = {
    val mapType = u.mapType
    val keyLoopVar = LambdaVariable("CatalystToExternalMap_key", mapType.keyType, nullable = false)
    val valueLoopVar =
      LambdaVariable("CatalystToExternalMap_value", mapType.valueType, mapType.valueContainsNull)
    CatalystToExternalMap(
      keyLoopVar,
      u.keyFunction(keyLoopVar),
      valueLoopVar,
      u.valueFunction(valueLoopVar),
      u.child,
      u.collClass
    )
  }
}
