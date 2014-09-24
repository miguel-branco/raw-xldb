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

import raw.catalog.TableCatalog

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
import raw.operators.Compute
import raw.operators.With
import raw.operators.Reuse
import raw.operators.Benchmark
import raw.operators.PrintBenchmark
import raw.operators.Print
import raw.operators.Histogram
import raw.operators.HistogramFill
import raw.operators.Count
import raw.operators.Sum
import raw.operators.First
import raw.operators.Last
import raw.operators.QueueInit
import raw.operators.QueuePush
import raw.operators.QueuePop
import raw.operators.UDFFilter

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

import raw.schema.Field
import raw.schema.Schema
import raw.schema.UINT64

object Optimizer {
  
  // FIXME: Turn into variable passed during recursion tree calls, such as with variable 'fields' in getPhysicalPlan()
  private var reuseFields = Map[String, Schema]()
  
  private var queueFields = Map[String, Schema]()
  
  private def getPhysicalPlan(operator: LogicalOperator, fields: List[String] = List()): PhysicalOperator = {
    
    /** Returns subset of 'fields' produced in a query tree given by 'operator'. */
    def _findFields(operator: LogicalOperator, fields: List[String]) : List[String] =
      operator match {
        case Load(_, _, query) => _findFields(query, fields)
        case Scan(tableName) => fields.filter(TableCatalog.table(tableName).hasField(_))
        case ScanByKey(tableName, child, _) => fields.filter(TableCatalog.table(tableName).hasField(_)) ++ _findFields(child, fields.filterNot(TableCatalog.table(tableName).hasField(_)))
        case Filter(child, _) => _findFields(child, fields)
        case InnerJoin(left, right, _, _) => _findFields(left, fields) ++ _findFields(right, fields)
        case AntiJoin(left, right, _, _) => _findFields(left, fields) ++ _findFields(right, fields)
        case Aggregate(child, _, aggregates) =>
          _findFields(child, fields) ++
          (for (aggregate <- aggregates; if fields.contains(aggregate)) 
            yield aggregate.alias)
        case UnionAll(children) => _findFields(children(0), fields)
        case Project(child, _) => _findFields(child, fields)
        case Compute(child, aliasedExpressions) =>
          // Distinct is required because Compute can create new fields (in aliasedExpressions) that are direct projections of fields produced below.
          (_findFields(child, fields) ++
          (for ((alias, _) <- aliasedExpressions; if fields.contains(alias))
            yield alias)).distinct
        case With(_, _, _) => List() 
        case Reuse(identifier) => fields.filter(reuseFields(identifier).names.contains(_))
        case Benchmark(_, child) => _findFields(child, fields)
        case PrintBenchmark(_, child) => _findFields(child, fields)
        case Print(child) => _findFields(child, fields)
        case Histogram(_, child) => _findFields(child, fields)
        case HistogramFill(_, child) => _findFields(child, fields)
        case QueueInit(_, child) => _findFields(child, fields)
        case QueuePush(_, child) => _findFields(child, fields)
        case QueuePop(identifier) => fields.filter(queueFields(identifier).names.contains(_))
        case UDFFilter(child) => _findFields(child, fields)
      }
  
    operator match {
        
      case Load(storage, fileNames, query) => {
        val physQuery = getPhysicalPlan(query, fields)
        PhysicalLoad(storage, fileNames, physQuery, physQuery.schema)
      }
      
      case Scan(tableName) => {
        val table = TableCatalog.table(tableName)
        val schema = new Schema(
          for (field <- fields)
            yield table.getField(field))
        PhysicalScan(table, schema)
      }
      
      case ScanByKey(tableName, child, key) => {
        val table = TableCatalog.table(tableName)
        val physChild = getPhysicalPlan(child, key)
        val keySchema = new Schema(
          for (field <- key)
            yield table.getField(field))
            // FIXME: Ensure that table.getField(field) == physChild.schema.getField(field)
        val schema = new Schema(
          for (field <- fields)
            yield table.getField(field))
        PhysicalScanByKey(table, physChild, keySchema, schema)
      }
      
      case Filter(child, expression) => {
        val childFields =
          (fields ++ expression.referencedFields).distinct
        val physChild = getPhysicalPlan(child, childFields)
        val schema = new Schema(
          for (field <- fields)
            yield physChild.schema.getField(field))
        PhysicalFilter(physChild, expression, schema)          
      }
      
      case InnerJoin(left, right, leftSelector, rightSelector) => {
        val leftProjector = _findFields(left, fields)
        val rightProjector = fields.filterNot(leftProjector.contains(_))
        val leftFields = (leftSelector ++ leftProjector).distinct
        val rightFields = (rightSelector ++ rightProjector).distinct
        val physLeftChild = getPhysicalPlan(left, leftFields)
        val physRightChild = getPhysicalPlan(right, rightFields)
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
        val physLeftChild = getPhysicalPlan(left, leftFields)
        val physRightChild = getPhysicalPlan(right, rightFields)
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
      
      case Aggregate(child, key, aggregates) => {
        val aggregateFields = 
          (for (aggregate <- aggregates) 
            yield aggregate.alias).toList
        val childFields =
          (key ++ 
          (for (field <- fields; if !aggregateFields.contains(field))
            yield field)).distinct
        val physChild = getPhysicalPlan(child, childFields)
        val keySchema = new Schema(
          for (field <- key)
            yield physChild.schema.getField(field))
        val schema =
          (keySchema ++
           new Schema(for (aggregate <- aggregates)
             yield aggregate match {
               // FIXME: Count can be nullable. Is this correct?
               case Count(alias) => Field(alias, UINT64, true)
               // FIXME: Sum takes same type as field being summed. Is this correct?
               case Sum(alias, field) => Field(alias, physChild.schema.getField(field).dataType, true)
               case First(alias, field) => Field(alias, physChild.schema.getField(field).dataType, true)
               case Last(alias, field) => Field(alias, physChild.schema.getField(field).dataType, true)
             }))
        GroupAggregate(physChild, keySchema, aggregates, schema)          
      }
      
      case UnionAll(children) => {
        val physChildren = 
          for (child <- children)
            yield getPhysicalPlan(child, fields)
        // FIXME: Is it necessary to verify that all children have the same schema & throw error otherwise?
        PhysicalUnionAll(physChildren, physChildren(0).schema)
      }
      
      case Project(child, projector) => {
        val childFields =
          (fields ++ projector).distinct
        getPhysicalPlan(child, childFields)
      }
      
      case Compute(child, aliasedExpressions) => {
        val aliasedFields = 
          (for ((alias, _) <- aliasedExpressions) 
            yield alias).toList
        val childFields =
          ((for (field <- fields; if !aliasedFields.contains(field))
            yield field) ++
          aliasedExpressions.flatMap(_._2.referencedFields)).distinct
        val physChild = getPhysicalPlan(child, childFields)
        val schema =
          new Schema(for ((alias, expression) <- aliasedExpressions)
            // FIXME: nullable always true?
            yield Field(alias, expression.dataType(physChild.schema), true))
        PhysicalCompute(physChild, aliasedExpressions, schema)
      }
      
      case With(identifier, withQuery, childQuery) => {
        val physWithQuery = getPhysicalPlan(withQuery)
        reuseFields += identifier -> physWithQuery.schema
        val physChildQuery = getPhysicalPlan(childQuery)
        PhysicalWith(identifier, physWithQuery, physChildQuery, physChildQuery.schema)
      }
        
      case Reuse(identifier) => {
        PhysicalReuse(identifier, reuseFields(identifier))
      }
      
      case Benchmark(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)
        StoreBenchmark(identifier, physChild, physChild.schema)
      }
      
      case PrintBenchmark(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)
        PrintBenchmarkToConsole(identifier, physChild, physChild.schema)
      }
        
      case Print(child) => {
        val physChild = getPhysicalPlan(child, fields)
        PrintToConsole(physChild, physChild.schema)
      }
      
      case Histogram(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)
        PhysicalHistogram(identifier, physChild, physChild.schema)
      }      
      
      case HistogramFill(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)
        PhysicalHistogramFill(identifier, physChild, physChild.schema)
      }      
      
      case QueueInit(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)        
        PhysicalQueueInit(identifier, physChild, physChild.schema)
      }
      
      case QueuePush(identifier, child) => {
        val physChild = getPhysicalPlan(child, fields)
        queueFields += identifier -> physChild.schema
        PhysicalQueuePush(identifier, physChild, physChild.schema)
      }
        
      case QueuePop(identifier) => {
        PhysicalQueuePop(identifier, queueFields(identifier))
      }
      
      case UDFFilter(child) => {
        val physChild = getPhysicalPlan(child, fields)
        PhysicalUDFFilter(physChild, physChild.schema)        
      }
    }
  }    
  
  def optimize(query: LogicalOperator) =
    getPhysicalPlan(query)
    
}