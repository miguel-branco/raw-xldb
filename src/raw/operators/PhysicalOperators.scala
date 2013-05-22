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
package raw.operators

import raw.catalog.Table

import raw.code.storage.Storage

import raw.expression.Expression

import raw.schema.Field
import raw.schema.Schema

// FIXME: Do the selectors & keys need to be of type Schema or is their name (String) enough? 

sealed abstract class PhysicalOperator(val schema: Schema)

case class LoadFiles(val storage: Storage,
                     val fileNames: List[String],
                     val query: List[PhysicalOperator],
                     s: Schema) extends PhysicalOperator(s)
case class ScanAndProject(val table: Table,
                          s: Schema) extends PhysicalOperator(s)
case class ScanByKeyAndProject(val table: Table,
                               val child: PhysicalOperator,
                               val keys: Schema,
                               s: Schema) extends PhysicalOperator(s)                          
case class FilterAndProject(val child: PhysicalOperator,
                            val expression: Expression,
                            s: Schema) extends PhysicalOperator(s)
case class HashUniqueInnerJoin(val left: PhysicalOperator, val right: PhysicalOperator,
                               val leftSelector: Schema, val rightSelector: Schema,
                               val leftProjector: Schema, val rightProjector: Schema,
                               s: Schema) extends PhysicalOperator(s)
case class HashUniqueAntiJoin(val left: PhysicalOperator, val right: PhysicalOperator,
                              val leftSelector: Schema, val rightSelector: Schema,
                              val leftProjector: Schema, val rightProjector: Schema,
                              s: Schema) extends PhysicalOperator(s)
case class GroupAggregate(val child: PhysicalOperator,
                          val keys: Schema,
                          val aggregates: List[AggregateFunction],
                          s: Schema) extends PhysicalOperator(s)
case class UnionAllAndProject(val children: List[PhysicalOperator],
                              s: Schema) extends PhysicalOperator(s)
case class StoreOutput(val identifier: String,
                       val child: PhysicalOperator,
                       s: Schema) extends PhysicalOperator(s)
case class ReuseOutput(val identifier: String,
                       s: Schema) extends PhysicalOperator(s)
case class StoreBenchmark(val identifier: String,
                          val child: PhysicalOperator,
                          s: Schema) extends PhysicalOperator(s)
case class PrintBenchmarkConsole(val identifier: String,
                                 val child: PhysicalOperator,
                                 s: Schema) extends PhysicalOperator(s)
case class PrintOutputConsole(val child: PhysicalOperator,
                              s: Schema) extends PhysicalOperator(s)
