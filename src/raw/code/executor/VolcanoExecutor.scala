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
import raw.operators.LoadFiles
import raw.operators.ScanAndProject
import raw.operators.ScanByKeyAndProject
import raw.operators.FilterAndProject
import raw.operators.HashUniqueInnerJoin
import raw.operators.HashUniqueAntiJoin
import raw.operators.GroupAggregate
import raw.operators.UnionAllAndProject
import raw.operators.StoreOutput
import raw.operators.ReuseOutput
import raw.operators.StoreBenchmark
import raw.operators.PrintBenchmarkConsole
import raw.operators.PrintOutputConsole

import raw.schema.Schema

/** Emits code that follows a Volcano model. */
abstract class VolcanoExecutor extends Executor {
  def emitBeginLoadFiles(ref: Reference, storage: Storage, fileNames: List[String], schema: Schema): List[Instruction]
  def emitEndLoadFiles(ref: Reference, child: Reference, schema: Schema): List[Instruction]
  def emitScanAndProject(ref: Reference, accessPath: SequentialAccessPath, schema: Schema): List[Instruction]
  def emitScanByKeyAndProject(ref: Reference, accessPath: KeyAccessPath, child: Reference, keys: Schema, schema: Schema): List[Instruction]
  def emitFilterAndProject(ref: Reference, child: Reference, expression: Expression, schema: Schema): List[Instruction] 
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
  def emitGroupAggregate(ref: Reference, child: Reference, keys: Schema, aggregates: List[AggregateFunction], schema: Schema): List[Instruction]                              
  def emitUnionAllAndProject(ref: Reference, children: List[Reference], schema: Schema): List[Instruction]
  def emitStoreOutput(identifier: String, child: Reference, schema: Schema): List[Instruction]
  def emitReuseOutput(ref: Reference, identifier: String, schema: Schema): List[Instruction]
  def emitStartBenchmark(identifier: String): List[Instruction]
  def emitStopBenchmark(identifier: String): List[Instruction]
  def emitPrintBenchmarkConsole(identifier: String): List[Instruction]
  def emitPrintOutputConsole(child: Reference): List[Instruction]
  
  def code(operator: PhysicalOperator): List[Instruction] = {
    def getReference(operator: PhysicalOperator): Reference =
      Reference(Util.getUniqueId())
    
    def _emit(operator: PhysicalOperator): Tuple2[List[Instruction], Reference] = {
      operator match {
        case LoadFiles(storage, fileNames, query, schema) => {
          val ref = getReference(operator)
          val beginCode = emitBeginLoadFiles(ref, storage, fileNames, schema)
          val tuples = query.map(_emit(_))
          val childrenCode = tuples.map(_._1)
          val childrenRef = tuples.map(_._2)
          val endCode = emitEndLoadFiles(ref, childrenRef.last, schema)
          (storage.init() ++ beginCode ++ childrenCode.foldLeft(List[Instruction]())(_ ++ _) ++ endCode ++ storage.done(), ref)
         }
        
        case ScanAndProject(table, schema) => {
          val ref = getReference(operator)
          val accessPath = table.getSequentialAccessPath(schema)
          val code = emitScanAndProject(ref, accessPath, schema)
          (accessPath.init() ++ code ++ accessPath.done(), ref)
        }
        case ScanByKeyAndProject(table, child, keys, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = getReference(operator)
          val accessPath = table.getKeyAccessPath(schema)
          val code = emitScanByKeyAndProject(ref, accessPath, childRef, keys, schema)
          (childCode ++ accessPath.init() ++ code ++ accessPath.done(), ref)
        }
        case FilterAndProject(child, expression, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = getReference(operator)
          val code = emitFilterAndProject(ref, childRef, expression, schema)
          (childCode ++ code, ref)
        }
        case HashUniqueInnerJoin(left, right, leftSelector, rightSelector, leftProjector, rightProjector, schema) => {
          val (leftCode, leftRef) = _emit(left)
          val (rightCode, rightRef) = _emit(right)
          val ref = getReference(operator)          
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
          val ref = getReference(operator)          
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
        case GroupAggregate(child, keys, aggregates, schema) => {
          val (childCode, childRef) = _emit(child)
          val ref = getReference(operator)                    
          val code = emitGroupAggregate(ref, childRef, keys, aggregates, schema)
          (childCode ++ code, ref)
        }
        case UnionAllAndProject(children, schema) => {
          val tuples = children.map(_emit(_))
          val childrenCode = tuples.map(_._1)
          val childrenRef = tuples.map(_._2)
          val ref = getReference(operator)                    
          val code = emitUnionAllAndProject(ref, childrenRef, schema)
          (childrenCode.foldLeft(List[Instruction]())(_ ++ _) ++ code, ref)
        }
        case StoreOutput(identifier, child, schema) => {
          val (childCode, childRef) = _emit(child)
          val code = emitStoreOutput(identifier, childRef, schema)
          (childCode ++ code, childRef)
        }
        case ReuseOutput(identifier, schema) => {
          val ref = getReference(operator)          
          val code = emitReuseOutput(ref, identifier, schema)
          (code, ref)
        }
        case StoreBenchmark(identifier, child, _) => {
          val preCode = emitStartBenchmark(identifier)
          val (childCode, childRef) = _emit(child)
          val postCode = emitStopBenchmark(identifier)
          (preCode ++ childCode ++ postCode, childRef)
        }
        case PrintBenchmarkConsole(identifier, child, _) => {
          val (childCode, childRef) = _emit(child)
          val code = emitPrintBenchmarkConsole(identifier)
          (childCode ++ code, childRef)
        }
        case PrintOutputConsole(child, _) => {
          val (childCode, childRef) = _emit(child)
          val code = emitPrintOutputConsole(childRef)
          (childCode ++ code, childRef)
        }
      }
    }
    val (childCode, ref) = _emit(operator)
    childCode
  }  
}

