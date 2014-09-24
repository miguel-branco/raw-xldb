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
package raw.code.executor

import raw.Util

import raw.code.Emitter
import raw.code.Instruction
import raw.code.Reference

import raw.code.storage.Storage
import raw.code.storage.SequentialAccessPath
import raw.code.storage.KeyAccessPath

import raw.expression.Expression

import raw.operators.AggregateFunction
import raw.operators.PhysicalOperator
import raw.operators.PhysicalLoad
import raw.operators.PhysicalScan
import raw.operators.PhysicalScanByKey
import raw.operators.PhysicalFilter
import raw.operators.HashUniqueInnerJoin
import raw.operators.HashUniqueAntiJoin
import raw.operators.GroupAggregate
import raw.operators.PhysicalUnionAll
import raw.operators.PhysicalCompute
import raw.operators.PhysicalWith
import raw.operators.PhysicalReuse
import raw.operators.StoreBenchmark
import raw.operators.PrintBenchmarkToConsole
import raw.operators.PrintToConsole
import raw.operators.PhysicalHistogram
import raw.operators.PhysicalHistogramFill
import raw.operators.PhysicalQueueInit
import raw.operators.PhysicalQueuePush
import raw.operators.PhysicalQueuePop
import raw.operators.PhysicalUDFFilter

import raw.schema.Schema

/** Emits code that follows a Volcano model. */
abstract class VolcanoExecutor extends Executor {
  def emitBeginLoad(ref: Reference, storage: Storage, fileNames: List[String], schema: Schema): List[Instruction]
  def emitEndLoad(ref: Reference, child: Reference, schema: Schema): List[Instruction]
  def emitScan(ref: Reference, accessPath: SequentialAccessPath, schema: Schema): List[Instruction]
  def emitScanByKey(ref: Reference, accessPath: KeyAccessPath, child: Reference, key: Schema, schema: Schema): List[Instruction]
  def emitFilter(ref: Reference, child: Reference, expression: Expression, schema: Schema): List[Instruction] 
  def emitHashUniqueInnerJoin(ref: Reference,
                              left: Reference, right: Reference,
                              leftSelector: Schema, rightSelector: Schema,
                              leftProjector: Schema, rightProjector: Schema,
                              schema: Schema): List[Instruction]
  def emitHashUniqueAntiJoin(ref: Reference,
                             left: Reference, right: Reference,
                             leftSelector: Schema, rightSelector: Schema,
                             leftProjector: Schema, rightProjector: Schema,
                             schema: Schema): List[Instruction]
  def emitGroupAggregate(ref: Reference, child: Reference, key: Schema, aggregates: List[AggregateFunction], schema: Schema): List[Instruction]                              
  def emitUnionAll(ref: Reference, children: List[Reference], schema: Schema): List[Instruction]
  def emitCompute(ref: Reference, child: Reference, aliasedExpressions: List[Tuple2[String, Expression]], schema: Schema): List[Instruction]
  def emitStore(ref: Reference, identifier: String): List[Instruction]
  def emitRelease(identifier: String): List[Instruction]
  def emitReuse(ref: Reference, identifier: String): List[Instruction]
  def emitStartBenchmark(identifier: String): List[Instruction]
  def emitStopBenchmark(identifier: String): List[Instruction]
  def emitPrintBenchmarkToConsole(identifier: String): List[Instruction]
  def emitPrintToConsole(child: Reference): List[Instruction]
  def emitHistogramNew(identifier: String): List[Instruction]
  def emitHistogramDraw(identifier: String): List[Instruction]
  def emitHistogramFill(identifier: String, child: Reference): List[Instruction]
  def emitQueueInit(identifier: String): List[Instruction]
  def emitQueuePush(identifier: String, ref: Reference): List[Instruction]
  def emitQueuePop(ref: Reference, identifier: String): List[Instruction]
  def emitUDFFilter(ref: Reference, child: Reference): List[Instruction]
  
  def code(query: PhysicalOperator): List[Instruction] = {
    def newReference(operator: PhysicalOperator): Reference =
      Reference(Util.getUniqueId())
    
    def _emit(operator: PhysicalOperator): Tuple2[List[Instruction], Reference] = {
      operator match {
        case PhysicalLoad(storage, fileNames, query, schema) => {
          val ref = newReference(operator)
          val beginCode = emitBeginLoad(ref, storage, fileNames, schema)
          val (queryCode, queryRef) = _emit(query)
          val endCode = emitEndLoad(ref, queryRef, schema)
          (storage.init() ++ beginCode ++ queryCode ++ endCode ++ storage.done(), ref)          
         }
        case PhysicalScan(table, schema) => {
          val ref = newReference(operator)
          val accessPath = table.getSequentialAccessPath(schema)
          val code = emitScan(ref, accessPath, schema)
          (accessPath.init() ++ code ++ accessPath.done(), ref)
        }
        case PhysicalScanByKey(table, child, key, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = newReference(operator)
          val accessPath = table.getKeyAccessPath(schema)
          val code = emitScanByKey(ref, accessPath, childRef, key, schema)
          (childCode ++ accessPath.init() ++ code ++ accessPath.done(), ref)
        }
        case PhysicalFilter(child, expression, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = newReference(operator)
          val code = emitFilter(ref, childRef, expression, schema)
          (childCode ++ code, ref)
        }
        case HashUniqueInnerJoin(left, right, leftSelector, rightSelector, leftProjector, rightProjector, schema) => {
          val (leftCode, leftRef) = _emit(left)
          val (rightCode, rightRef) = _emit(right)
          val ref = newReference(operator)          
          val code = emitHashUniqueInnerJoin(ref,
                                             leftRef,
                                             rightRef,
                                             leftSelector,
                                             rightSelector,
                                             leftProjector,
                                             rightProjector,
                                             schema)
          (leftCode ++ rightCode ++ code, ref)
        }
        case HashUniqueAntiJoin(left, right, leftSelector, rightSelector, leftProjector, rightProjector, schema) => {
          val (leftCode, leftRef) = _emit(left)
          val (rightCode, rightRef) = _emit(right)
          val ref = newReference(operator)          
          val code = emitHashUniqueAntiJoin(ref,
                                            leftRef,
                                            rightRef,
                                            leftSelector,
                                            rightSelector,
                                            leftProjector,
                                            rightProjector,
                                            schema)
          (leftCode ++ rightCode ++ code, ref)
        }        
        case GroupAggregate(child, key, aggregates, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = newReference(operator)                    
          val code = emitGroupAggregate(ref, childRef, key, aggregates, schema)
          (childCode ++ code, ref)
        }
        case PhysicalUnionAll(children, schema) => {
          val tuples = children.map(_emit(_))
          val childrenCode = tuples.map(_._1)
          val childrenRef = tuples.map(_._2)
          val ref = newReference(operator)                    
          val code = emitUnionAll(ref, childrenRef, schema)
          (childrenCode.foldLeft(List[Instruction]())(_ ++ _) ++ code, ref)
        }
        case PhysicalCompute(child, aliasedExpressions, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = newReference(operator)
          val code = emitCompute(ref, childRef, aliasedExpressions, schema)
          (childCode ++ code, ref)
        }
        case PhysicalWith(identifier, withQuery, childQuery, schema) => {
          val (withCode, withRef) = _emit(withQuery)
          val storeCode = emitStore(withRef, identifier)          
          val (childCode, childRef) = _emit(childQuery)
          val releaseCode = emitRelease(identifier)
          (withCode ++ storeCode ++ childCode ++ releaseCode, childRef)
        }
        case PhysicalReuse(identifier, schema) => {
          val ref = newReference(operator)
          val code = emitReuse(ref, identifier)
          (code, ref)
        }
        case StoreBenchmark(identifier, child, _) => {
          val preCode = emitStartBenchmark(identifier)
          val (childCode, childRef) = _emit(child)
          val postCode = emitStopBenchmark(identifier)
          (preCode ++ childCode ++ postCode, childRef)
        }
        case PrintBenchmarkToConsole(identifier, child, _) => {
          val (childCode, childRef) = _emit(child)
          val code = emitPrintBenchmarkToConsole(identifier)
          (childCode ++ code, childRef)
        }
        case PrintToConsole(child, _) => {
          val (childCode, childRef) = _emit(child)
          val code = emitPrintToConsole(childRef)
          (childCode ++ code, childRef)
        }
        case PhysicalHistogram(identifier, child, _) => {
          val newHistoCode = emitHistogramNew(identifier)
          val (childCode, childRef) = _emit(child)
          val drawHistoCode = emitHistogramDraw(identifier)
          (newHistoCode ++ childCode ++ drawHistoCode, childRef)
        }
        case PhysicalHistogramFill(identifier, child, _) => {
          val (childCode, childRef) = _emit(child)
          val code = emitHistogramFill(identifier, childRef)
          (childCode ++ code, childRef)          
        }
        case PhysicalQueueInit(identifier, child, schema) => {
          val queueCode = emitQueueInit(identifier)
          val (childCode, childRef) = _emit(child)
          (queueCode ++ childCode, childRef)
        }
        case PhysicalQueuePush(identifier, child, schema) => {
          val (childCode, childRef) = _emit(child)
          val queueCode = emitQueuePush(identifier, childRef)
          (childCode ++ queueCode, childRef)
        }
        case PhysicalQueuePop(identifier, schema) => {
          val ref = newReference(operator)
          val queueCode = emitQueuePop(ref, identifier)
          (queueCode, ref)
        }
        case PhysicalUDFFilter(child, schema) => {
          val (childCode, childRef) = _emit(child)          
          val ref = newReference(operator)
          val udfCode = emitUDFFilter(ref, childRef)
          (childCode ++ udfCode, ref)
        }
      }
    }
    
    val (code, ref) = _emit(query)
    code ++ emitRelease(ref.name)
  }  
}




