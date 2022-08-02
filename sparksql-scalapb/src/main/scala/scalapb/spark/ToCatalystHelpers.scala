package scalapb.spark

import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, MapObjects, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{
  CreateNamedStruct,
  Expression,
  If,
  IsNull,
  Literal
}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  IntegerType,
  MapType,
  ObjectType,
  StructType,
  TimestampType
}
import scalapb.descriptors.{Descriptor, FieldDescriptor, PValue, ScalaType}
import scalapb.GeneratedMessageCompanion
import org.apache.spark.sql.catalyst.expressions.objects.ExternalMapToCatalyst
import scalapb.GeneratedMessage

trait ToCatalystHelpers {
  def protoSql: ProtoSQL

  def schemaOptions: SchemaOptions

  def messageToCatalyst(
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
    if (protoSql.schemaOptions.isUnpackedPrimitiveWrapper(cmp.scalaDescriptor)) {
      val fd = cmp.scalaDescriptor.fields(0)
      fieldToCatalyst(cmp, fd, input)
    } else if (
      protoSql.schemaOptions.sparkTimestamps && cmp.scalaDescriptor.fullName == "google.protobuf.Timestamp"
    ) {
      val secondsFd: FieldDescriptor = cmp.scalaDescriptor.fields(0)
      val nanosFd: FieldDescriptor = cmp.scalaDescriptor.fields(1)
      val secondsExpr = fieldToCatalyst(cmp, secondsFd, input)
      val nanosExpr = fieldToCatalyst(cmp, nanosFd, input)
      StaticInvoke(
        JavaTimestampHelpers.getClass,
        TimestampType,
        "combineSecondsAndNanosIntoMicrosTimestamp",
        secondsExpr :: nanosExpr :: Nil
      )
    } else {
      val nameExprs =
        cmp.scalaDescriptor.fields.filter(field => containsColumn(field.name)).map { field =>
          Literal(schemaOptions.columnNaming.fieldName(field))
        }

      val valueExprs =
        cmp.scalaDescriptor.fields.filter(field => containsColumn(field.name)).map { field =>
          fieldToCatalyst(
            cmp,
            field,
            input,
            dataTypeOpt.map(dataType => dataType(field.name).dataType)
          )
        }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap { case (nameExpr, valueExpr) =>
        nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(input), nullExpr, createExpr)
    }
  }

  def fieldGetterAndTransformer(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      dataType: Option[DataType] = None
  ): (Expression => Expression, Expression => Expression) = {
    def messageFieldCompanion = cmp.messageCompanionForFieldNumber(fd.number)

    val isMessage = fd.scalaType.isInstanceOf[ScalaType.Message]

    def getField(inputObject: Expression): Expression =
      Invoke(
        inputObject,
        "getField",
        ObjectType(classOf[PValue]),
        Invoke(
          Invoke(
            Invoke(
              Literal.fromObject(cmp),
              "scalaDescriptor",
              ObjectType(classOf[Descriptor]),
              Nil
            ),
            "findFieldByNumber",
            ObjectType(classOf[Option[_]]),
            Literal(fd.number) :: Nil
          ),
          "get",
          ObjectType(classOf[FieldDescriptor])
        ) :: Nil
      )

    def getFieldByNumber(inputObject: Expression): Expression =
      Invoke(
        inputObject,
        "getFieldByNumber",
        if (fd.isRepeated)
          ObjectType(classOf[Seq[_]])
        else
          ObjectType(messageFieldCompanion.defaultInstance.getClass),
        Literal(fd.number, IntegerType) :: Nil
      )

    if (!isMessage) {
      (getField, { e: Expression => singularFieldToCatalyst(fd, e) })
    } else {
      (
        getFieldByNumber,
        { e: Expression =>
          messageToCatalyst(messageFieldCompanion, e, dataType.map(_.asInstanceOf[StructType]))
        }
      )
    }
  }

  def fieldToCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      inputObject: Expression,
      dataType: Option[DataType] = None
  ): Expression = {

    val isMessage = fd.scalaType.isInstanceOf[ScalaType.Message]

    val (fieldGetter, transform) = fieldGetterAndTransformer(cmp, fd, dataType)

    def messageFieldCompanion = cmp.messageCompanionForFieldNumber(fd.number)

    if (fd.isRepeated) {
      if (fd.isMapField) {
        val keyDesc =
          fd.scalaType.asInstanceOf[ScalaType.Message].descriptor.findFieldByNumber(1).get
        val valueDesc =
          fd.scalaType.asInstanceOf[ScalaType.Message].descriptor.findFieldByNumber(2).get
        val (_, valueTransform) =
          fieldGetterAndTransformer(
            cmp.messageCompanionForFieldNumber(fd.number),
            valueDesc,
            dataType.map(_.asInstanceOf[MapType].valueType)
          )
        val valueType = valueDesc.scalaType match {
          case ScalaType.Message(_) => ObjectType(classOf[GeneratedMessage])
          case _                    => ObjectType(classOf[PValue])
        }

        ExternalMapToCatalyst(
          StaticInvoke(
            JavaHelpers.getClass,
            ObjectType(classOf[Map[_, _]]),
            "mkMap",
            fieldGetter(inputObject) :: Nil
          ),
          ObjectType(classOf[PValue]),
          singularFieldToCatalyst(keyDesc, _),
          false,
          valueType,
          valueTransform,
          true
        )
      } else if (isMessage) {
        val (fieldGetter, transform) =
          fieldGetterAndTransformer(cmp, fd, dataType.map(_.asInstanceOf[ArrayType].elementType))
        MapObjects(
          transform,
          fieldGetter(inputObject),
          ObjectType(messageFieldCompanion.defaultInstance.getClass)
        )
      } else {
        val getter = StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[Vector[_]]),
          "vectorFromPValue",
          fieldGetter(inputObject) :: Nil
        )
        MapObjects(transform, getter, ObjectType(classOf[PValue]))
      }
    } else {
      if (isMessage) transform(fieldGetter(inputObject))
      else
        If(
          StaticInvoke(
            JavaHelpers.getClass,
            BooleanType,
            "isEmpty",
            fieldGetter(inputObject) :: Nil
          ),
          Literal.create(null, protoSql.dataTypeFor(fd)),
          transform(fieldGetter(inputObject))
        )
    }
  }

  def singularFieldToCatalyst(
      fd: FieldDescriptor,
      input: Expression
  ): Expression = {
    val obj = fd.scalaType match {
      case ScalaType.Int        => "intFromPValue"
      case ScalaType.Long       => "longFromPValue"
      case ScalaType.Float      => "floatFromPValue"
      case ScalaType.Double     => "doubleFromPValue"
      case ScalaType.Boolean    => "booleanFromPValue"
      case ScalaType.String     => "stringFromPValue"
      case ScalaType.ByteString => "byteStringFromPValue"
      case ScalaType.Enum(_)    => "enumFromPValue"
      case ScalaType.Message(_) =>
        throw new RuntimeException("Should not happen")
    }
    StaticInvoke(
      JavaHelpers.getClass,
      protoSql.singularDataType(fd),
      obj,
      input :: Nil
    )
  }
}
