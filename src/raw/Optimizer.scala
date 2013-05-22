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
package raw

import scala.collection.mutable.Map

import raw.operators.Count

import raw.operators.LogicalOperator
import raw.operators.Load
import raw.operators.Scan
import raw.operators.ScanByKey
import raw.operators.Filter
import raw.operators.InnerJoin
import raw.operators.AntiJoin
import raw.operators.Aggregate
import raw.operators.UnionAll
import raw.operators.Project
import raw.operators.Store
import raw.operators.Reuse
import raw.operators.Benchmark
import raw.operators.PrintBenchmark
import raw.operators.Print

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

import raw.schema.Field
import raw.schema.Schema
import raw.schema.UINT64

object Optimizer {
  
  private def getPhysicalPlans(roots: List[LogicalOperator]) = {
    
    var reuseFields = Map[String, Schema]()
    
    def _apply(operator: LogicalOperator, fields: List[String]): PhysicalOperator = {
        
      /** Returns subset of 'fields' produced in a query tree given by 'operator'. */
      def _findFields(operator: LogicalOperator, fields: List[String]) : List[String] =            
        operator match {
          case Load(_, _, query) => query.flatMap(_findFields(_, fields))
          case Scan(table) => fields.filter(table.hasField(_))
          case ScanByKey(table, child, _) => fields.filter(table.hasField(_)) ++ _findFields(child, fields.filterNot(table.hasField(_)))
          case Filter(child, _) => _findFields(child, fields)
          case InnerJoin(left, right, _, _) => _findFields(left, fields) ++ _findFields(right, fields)
          case AntiJoin(left, right, _, _) => _findFields(left, fields) ++ _findFields(right, fields)
          case Aggregate(child, _, aggregates) =>
            _findFields(child, fields) ++
            (for (aggregate <- aggregates) 
              yield aggregate match {
                case Count(alias) => alias
            })
          case UnionAll(children) => children.flatMap(_findFields(_, fields))
          case Project(child, _) => _findFields(child, fields)
          case Store(_, child) => _findFields(child, fields)
          case Reuse(identifier) => reuseFields(identifier).names
          case Benchmark(_, child) => _findFields(child, fields)
          case PrintBenchmark(_, child) => _findFields(child, fields)
          case Print(child) => _findFields(child, fields)
        }
    
      operator match {
          
        case Load(storage, fileNames, query) => {
          val physQuery = 
            for (q <- query)
              yield _apply(q, fields)
          LoadFiles(storage, fileNames, physQuery, physQuery.last.schema)
        }
        
        case Scan(table) => {
          val schema = new Schema(
            for (field <- fields)
              yield table.getField(field))
          ScanAndProject(table, schema)
        }
        
        case ScanByKey(table, child, keys) => {
          val physChild = _apply(child, keys)
          val keysSchema = new Schema(
            for (field <- keys)
              yield table.getField(field))
              // FIXME: Ensure that table.getField(field) == physChild.schema.getField(field)
          val schema = new Schema(
            for (field <- fields)
              yield table.getField(field))
          ScanByKeyAndProject(table, physChild, keysSchema, schema)
        }
        
        case Filter(child, expression) => {
          val childFields =
            (fields ++ expression.getFields).distinct
          val physChild = _apply(child, childFields)
          val schema = new Schema(
            for (field <- fields)
              yield physChild.schema.getField(field))
          FilterAndProject(physChild, expression, schema)          
        }
        
        case InnerJoin(left, right, leftSelector, rightSelector) => {
          val leftProjector = _findFields(left, fields)
          val rightProjector = fields.filterNot(leftProjector.contains(_))
          val leftFields = (leftSelector ++ leftProjector).distinct
          val rightFields = (rightSelector ++ rightProjector).distinct
          val physLeftChild = _apply(left, leftFields)
          val physRightChild = _apply(right, rightFields)
          val leftSelectorSchema = new Schema(
            for (field <- leftSelector)
              yield physLeftChild.schema.getField(field))
          val rightSelectorSchema = new Schema(
            for (field <- rightSelector)
              yield physRightChild.schema.getField(field))         
          val leftProjectorSchema = new Schema(
              for (field <- leftProjector)
                yield physLeftChild.schema.getField(field))
          val rightProjectorSchema = new Schema(
              for (field <- rightProjector)
                yield physRightChild.schema.getField(field))
          val schema = leftProjectorSchema ++ rightProjectorSchema
          HashUniqueInnerJoin(physLeftChild, physRightChild, leftSelectorSchema, rightSelectorSchema, leftProjectorSchema, rightProjectorSchema, schema)
        }
        
        case AntiJoin(left, right, leftSelector, rightSelector) => {
          val leftProjector = _findFields(left, fields)
          val rightProjector = fields.filterNot(leftProjector.contains(_))
          val leftFields = (leftSelector ++ leftProjector).distinct
          val rightFields = (rightSelector ++ rightProjector).distinct
          val physLeftChild = _apply(left, leftFields)
          val physRightChild = _apply(right, rightFields)
          val leftSelectorSchema = new Schema(
            for (field <- leftSelector)
              yield physLeftChild.schema.getField(field))
          val rightSelectorSchema = new Schema(
            for (field <- rightSelector)
              yield physRightChild.schema.getField(field))         
          val leftProjectorSchema = new Schema(
              for (field <- leftProjector)
                yield physLeftChild.schema.getField(field))
          val rightProjectorSchema = new Schema(
              for (field <- rightProjector)
                yield physRightChild.schema.getField(field))
          val schema = leftProjectorSchema ++ rightProjectorSchema
          HashUniqueAntiJoin(physLeftChild, physRightChild, leftSelectorSchema, rightSelectorSchema, leftProjectorSchema, rightProjectorSchema, schema)
        }
        
        case Aggregate(child, keys, aggregates) => {
          val aggregateFields = 
            (for (aggregate <- aggregates) 
              yield aggregate match {
                case Count(alias) => alias
              }).toList
          val childFields =
            (keys ++ 
            (for (field <- fields; if !aggregateFields.contains(field))
              yield field)).distinct
          val physChild = _apply(child, childFields)
          val keysSchema = new Schema(
            for (field <- keys)
              yield physChild.schema.getField(field))
          val schema =
            (keysSchema ++
             new Schema(for (aggregate <- aggregates)
               yield aggregate match {
                 case Count(alias) => Field(alias, UINT64, true)
               }))
          GroupAggregate(physChild, keysSchema, aggregates, schema)          
        }
        
        case UnionAll(children) => {
          val physChildren = 
            for (child <- children)
              yield _apply(child, fields)
          // FIXME: Is it necessary to verify that all children have the same schema & throw error otherwise?
          UnionAllAndProject(physChildren, physChildren(0).schema)
        }
        
        case Store(identifier, child) => {
          val physChild = _apply(child, fields)
          reuseFields += identifier -> physChild.schema
          StoreOutput(identifier, physChild, physChild.schema)
        }
          
        case Reuse(identifier) => {
          ReuseOutput(identifier, reuseFields(identifier))
        }
        
        case Project(child, projector) => {
          val childFields =
            (fields ++ projector).distinct
          _apply(child, childFields)
        }
        
        case Benchmark(identifier, child) => {
          val physChild = _apply(child, fields)
          StoreBenchmark(identifier, physChild, physChild.schema)
        }
        
        case PrintBenchmark(identifier, child) => {
          val physChild = _apply(child, fields)
          PrintBenchmarkConsole(identifier, physChild, physChild.schema)
        }
          
        case Print(child) => {
          val physChild = _apply(child, fields)
          PrintOutputConsole(physChild, physChild.schema)
        }
      }
    }    
      
    // FIXME: Assumes 'roots' is in topological order, i.e. plans with Store are built before plans with Reuse 
    for (root <- roots)
      yield _apply(root, List())
  }
  
  def optimize(query: List[LogicalOperator]) =
    getPhysicalPlans(query)
    
}