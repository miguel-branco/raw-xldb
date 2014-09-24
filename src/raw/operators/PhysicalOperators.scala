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

case class PhysicalLoad(val storage: Storage,
                        val fileNames: List[String],
                        val query: PhysicalOperator,
                        _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalScan(val table: Table,
                        _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalScanByKey(val table: Table,
                             val child: PhysicalOperator,
                             val key: Schema,
                             _schema: Schema) extends PhysicalOperator(_schema)                          
case class PhysicalFilter(val child: PhysicalOperator,
                          val expression: Expression,
                          _schema: Schema) extends PhysicalOperator(_schema)
case class HashUniqueInnerJoin(val left: PhysicalOperator, val right: PhysicalOperator,
                               val leftSelector: Schema, val rightSelector: Schema,
                               val leftProjector: Schema, val rightProjector: Schema,
                               _schema: Schema) extends PhysicalOperator(_schema)
case class HashUniqueAntiJoin(val left: PhysicalOperator, val right: PhysicalOperator,
                              val leftSelector: Schema, val rightSelector: Schema,
                              val leftProjector: Schema, val rightProjector: Schema,
                              _schema: Schema) extends PhysicalOperator(_schema)
case class GroupAggregate(val child: PhysicalOperator,
                          val key: Schema,
                          val aggregates: List[AggregateFunction],
                          _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalUnionAll(val children: List[PhysicalOperator],
                            _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalCompute(val child: PhysicalOperator,
                           val aliasedExpressions: List[Tuple2[String, Expression]],
                           _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalWith(val identifier: String,
                        val withQuery: PhysicalOperator,
                        val childQuery: PhysicalOperator,
                        _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalReuse(val identifier: String,
                         _schema: Schema) extends PhysicalOperator(_schema)
case class StoreBenchmark(val identifier: String,
                          val child: PhysicalOperator,
                          _schema: Schema) extends PhysicalOperator(_schema)
case class PrintBenchmarkToConsole(val identifier: String,
                                   val child: PhysicalOperator,
                                   _schema: Schema) extends PhysicalOperator(_schema)
case class PrintToConsole(val child: PhysicalOperator,
                          _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalHistogram(val identifier: String,
                             val child: PhysicalOperator,
                             _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalHistogramFill(val identifier: String,
                                 val child: PhysicalOperator,
                                 _schema: Schema) extends PhysicalOperator(_schema)

case class PhysicalQueueInit(val identifier: String,
                             val child: PhysicalOperator,
                             _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalQueuePush(val identifier: String,
                             val child: PhysicalOperator,
                             _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalQueuePop(val identifier: String,
                            _schema: Schema) extends PhysicalOperator(_schema)
case class PhysicalUDFFilter(val child: PhysicalOperator,
                             _schema: Schema) extends PhysicalOperator(_schema)
