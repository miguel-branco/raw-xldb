/*
   RAW -- High-performance querying over raw, never-seen-before data.
   
                         Copyright (c) 2013
      Data Intensive Applications and Systems Labaratory (DIAS)
               École Polytechnique Fédérale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/
package raw.code.executor.supersonic

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder

import raw.Util

import raw.code.Emitter
import raw.code.Instruction
import raw.code.Reference

import raw.code.executor.VolcanoExecutor

import raw.code.storage.Storage
import raw.code.storage.SequentialAccessPath
import raw.code.storage.KeyAccessPath

import raw.expression.Expression
import raw.expression.FieldReference
import raw.expression.ConstFloat
import raw.expression.ConstInt32
import raw.expression.Abs
import raw.expression.Cos
import raw.expression.Sin
import raw.expression.Sinh
import raw.expression.Sqrt
import raw.expression.Negate
import raw.expression.Plus
import raw.expression.Minus
import raw.expression.Multiply
import raw.expression.Divide
import raw.expression.And
import raw.expression.Or
import raw.expression.Equal
import raw.expression.NotEqual
import raw.expression.Less
import raw.expression.LessOrEqual
import raw.expression.Greater
import raw.expression.GreaterOrEqual
import raw.expression.If

import raw.operators.AggregateFunction
import raw.operators.Count
import raw.operators.Sum
import raw.operators.First
import raw.operators.Last

import raw.schema.Schema
import raw.schema.DataType
import raw.schema.ROWID
import raw.schema.UINT32
import raw.schema.INT32
import raw.schema.FLOAT
import raw.schema.BOOL

// FIXME: Consider nested macros for code generation, i.e. macros that take code macros as input. Or use Helper classes and assign the implementation only, i.e. code generate smtg resembling the Volcano model.

class SupersonicExecutor extends VolcanoExecutor {

  private def getSupersonicType(dataType: DataType) = 
    dataType match {
      case ROWID => "kRowidDatatype"
      case _ => dataType.toString()
    }
  
  private def getSupersonicNullable(nullable: Boolean) =
    if (nullable) "NULLABLE" else "NOT_NULLABLE"
  
  def init(): List[Instruction] = List()
    
  def done(): List[Instruction] = List()
  
  private def createSupersonicTable(tableName: String, schema: Schema): List[Instruction] =
    // FIXME: Is it necessary to valid schema against table's schema
    List(Instruction("SUPERSONIC_SCHEMA_NEW", tableName)) ++
    schema.fields.map(field => Instruction("SUPERSONIC_SCHEMA_ADD", tableName, field.name, getSupersonicType(field.dataType), getSupersonicNullable(field.nullable))) ++
    List(Instruction("SUPERSONIC_TABLE_NEW", tableName))

  def emitBeginLoad(ref: Reference, storage: Storage, fileNames: List[String], schema: Schema): List[Instruction] =
    createSupersonicTable(ref.name, schema) ++
    List(Instruction("LOAD_FILES_LOOP_BEGIN", storage.reference.name, fileNames.length.toString(), "\"" + fileNames.mkString("\",\"") + "\""))

  def emitEndLoad(ref: Reference, child: Reference, schema: Schema): List[Instruction] =
    List(Instruction("LOAD_FILES_LOOP_END", ref.name, child.name))
    
  def emitScan(ref: Reference, accessPath: SequentialAccessPath, schema: Schema): List[Instruction] =
    createSupersonicTable(ref.name, schema) ++
    accessPath.open() ++
    accessPath.getNextTuple() ++
    List(Instruction("SUPERSONIC_TABLE_ADD", ref.name)) ++
    (for ((field, col) <- schema.fields.zipWithIndex)
      yield Instruction("SUPERSONIC_TABLE_SET", ref.name, col.toString, getSupersonicType(field.dataType), accessPath.getField(field).name)) ++
    accessPath.close()
   
  def emitScanByKey(ref: Reference, accessPath: KeyAccessPath, child: Reference, key: Schema, schema: Schema): List[Instruction] = {
    val uniqueId = Util.getUniqueId()
    createSupersonicTable(ref.name, schema) ++
    List(Instruction("SUPERSONIC_COLUMN_LOOP_BEGIN", uniqueId, child.name, key.names(0), "kRowidDatatype", "rowid_t")) ++
    accessPath.open(Reference(uniqueId)) ++
    accessPath.getNextTuple() ++
    List(Instruction("SUPERSONIC_TABLE_ADD", ref.name)) ++
    (for ((field, col) <- schema.fields.zipWithIndex)
      yield Instruction("SUPERSONIC_TABLE_SET", ref.name, col.toString, getSupersonicType(field.dataType), accessPath.getField(field).name)) ++
    accessPath.close(Reference(uniqueId)) ++
    List(Instruction("SUPERSONIC_COLUMN_LOOP_END"))
  }

  private def emitExpression(expression: Expression): String =
    // FIXME: There ought to be a way to auto-generate these names. Group them by number of arguments?
    expression match {
      case FieldReference(name) => "NamedAttribute(\"" + name + "\")"
      case ConstFloat(value) => "ConstFloat(" + value + ")"
      case ConstInt32(value) => "ConstInt32(" + value + ")"
      case Abs(child) => "Abs(" + emitExpression(child) + ")"
      case Cos(child) => "Cos(" + emitExpression(child) + ")"
      case Sin(child) => "Sin(" + emitExpression(child) + ")"
      case Sinh(child) => "Sinh(" + emitExpression(child) + ")"
      case Sqrt(child) => "SqrtQuiet(" + emitExpression(child) + ")"
      case Negate(child) => "Negate(" + emitExpression(child) + ")"
      case Plus(left, right) => "Plus(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Minus(left, right) => "Minus(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Multiply(left, right) => "Multiply(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Divide(left, right) => "Divide(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case And(left, right) => "And(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Or(left, right) => "Or(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Equal(left, right) => "Equal(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case NotEqual(left, right) => "NotEqual(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Less(left, right) => "Less(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case LessOrEqual(left, right) => "LessOrEqual(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case Greater(left, right) => "Greater(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case GreaterOrEqual(left, right) => "GreaterOrEqual(" + emitExpression(left) + "," + emitExpression(right) + ")"
      case If(first, second, third) => "If(" + emitExpression(first) + "," + emitExpression(second) + "," + emitExpression(third) + ")"
    }
  
  def emitFilter(ref: Reference, child: Reference, expression: Expression, schema: Schema): List[Instruction] =
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", ref.name)) ++
    schema.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", ref.name, _)) ++
    List(Instruction("SUPERSONIC_FILTER", ref.name, emitExpression(expression), child.name))
 
  def emitHashJoin(ref: Reference, joinType: String, unique: Boolean, left: Reference, right: Reference, leftSelector: Schema, rightSelector: Schema, leftProjector: Schema, rightProjector: Schema): List[Instruction] =
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", "lhs_selector_" + ref.name)) ++
    leftSelector.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", "lhs_selector_" + ref.name, _)) ++
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", "rhs_selector_" + ref.name)) ++
    rightSelector.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", "rhs_selector_" + ref.name, _)) ++
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", "lhs_" + ref.name)) ++
    leftProjector.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", "lhs_" + ref.name, _)) ++
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", "rhs_" + ref.name)) ++
    rightProjector.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", "rhs_selector_" + ref.name, _)) ++
    (if (unique)
      List(Instruction("SUPERSONIC_HASH_JOIN", ref.name, left.name, right.name, joinType, "UNIQUE"))
    else
      List(Instruction("SUPERSONIC_HASH_JOIN", ref.name, left.name, right.name, joinType, "NOT_UNIQUE")))
  
  def emitHashUniqueInnerJoin(ref: Reference, left: Reference, right: Reference, leftSelector: Schema, rightSelector: Schema, leftProjector: Schema, rightProjector: Schema, schema: Schema): List[Instruction] =
    emitHashJoin(ref, "INNER", true, left, right, leftSelector, rightSelector, leftProjector, rightProjector)
  
  def emitHashUniqueAntiJoin(ref: Reference, left: Reference, right: Reference, leftSelector: Schema, rightSelector: Schema, leftProjector: Schema, rightProjector: Schema, schema: Schema): List[Instruction] = 
    emitHashJoin(ref, "ANTI", true, left, right, leftSelector, rightSelector, leftProjector, rightProjector)
  
  def emitGroupAggregate(ref: Reference, child: Reference, key: Schema, aggregates: List[AggregateFunction], schema: Schema): List[Instruction] =                              
    List(Instruction("SUPERSONIC_PROJECTOR_NEW", "agg_key_" + ref.name)) ++
    key.names.map(Instruction("SUPERSONIC_PROJECTOR_ADD", "agg_key_" + ref.name, _)) ++
    List(Instruction("SUPERSONIC_AGGREGATOR_NEW", ref.name)) ++
    (for (aggregate <- aggregates)
      yield aggregate match {
        case Count(alias) => Instruction("SUPERSONIC_AGGREGATOR_ADD", ref.name, "COUNT", "", alias)
        case Sum(alias, field) => Instruction("SUPERSONIC_AGGREGATOR_ADD", ref.name, "SUM", field, alias)
        case First(alias, field) => Instruction("SUPERSONIC_AGGREGATOR_ADD", ref.name, "FIRST", field, alias)
        case Last(alias, field) => Instruction("SUPERSONIC_AGGREGATOR_ADD", ref.name, "LAST", field, alias)
      }) ++
    List(Instruction("SUPERSONIC_GROUP_AGGREGATE", ref.name, child.name))
  
  def emitUnionAll(ref: Reference, children: List[Reference], schema: Schema): List[Instruction] =
    createSupersonicTable(ref.name, schema) ++
    children.map(child => Instruction("SUPERSONIC_UNION", ref.name, child.name))
  
  def emitCompute(ref: Reference, child: Reference, aliasedExpressions: List[Tuple2[String, Expression]], schema: Schema): List[Instruction] =
    List(Instruction("SUPERSONIC_COMPUTOR_NEW", ref.name)) ++
    (for ((alias, expression) <- aliasedExpressions)
      yield Instruction("SUPERSONIC_COMPUTOR_ADD", ref.name, alias, emitExpression(expression))) ++
    List(Instruction("SUPERSONIC_COMPUTE", ref.name, child.name))
    
  def emitStore(ref: Reference, identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_STORE", ref.name, identifier))
    
  def emitRelease(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_RELEASE", identifier))

  def emitReuse(ref: Reference, identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_REUSE", ref.name, identifier))
  
  def emitStartBenchmark(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_BENCHMARK_START", identifier))
    
  def emitStopBenchmark(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_BENCHMARK_STOP", identifier))

  def emitPrintBenchmarkToConsole(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_BENCHMARK_PRINT", identifier))

  def emitPrintToConsole(child: Reference): List[Instruction] =
    List(Instruction("SUPERSONIC_PRINT", child.name))    

  def emitHistogramNew(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_HISTOGRAM_NEW", identifier))
    
  def emitHistogramDraw(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_HISTOGRAM_DRAW", identifier))
    
  def emitHistogramFill(identifier: String, child: Reference): List[Instruction] =
    List(Instruction("SUPERSONIC_HISTOGRAM_FILL", identifier, child.name))    
    
  def emitQueueInit(identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_QUEUE_INIT", identifier))
    
  def emitQueuePush(identifier: String, ref: Reference): List[Instruction] =
    List(Instruction("SUPERSONIC_QUEUE_PUSH", identifier, ref.name))
    
  def emitQueuePop(ref: Reference, identifier: String): List[Instruction] =
    List(Instruction("SUPERSONIC_QUEUE_POP", identifier, ref.name))
    
  def emitUDFFilter(ref: Reference, child: Reference): List[Instruction] =
    List(Instruction("DEMO_UDF", ref.name, child.name))
}
