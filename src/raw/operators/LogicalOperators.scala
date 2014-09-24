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

import raw.expression.Expression

import raw.code.storage.Storage

sealed abstract class AggregateFunction(val alias: String)
case class Count(_alias: String) extends AggregateFunction(_alias)
case class Sum(_alias: String, val field: String) extends AggregateFunction(_alias)
case class First(_alias: String, val field: String) extends AggregateFunction(_alias)
case class Last(_alias: String, val field: String) extends AggregateFunction(_alias)

sealed abstract class LogicalOperator

// FIXME: Pass fileNames as sub-select instead of list[String]?
case class Load(val storage: Storage,
                val fileNames: List[String],
                val query: LogicalOperator) extends LogicalOperator
case class Scan(val tableName: String) extends LogicalOperator
case class ScanByKey(val tableName: String,
                     val child: LogicalOperator,
                     val key: List[String]) extends LogicalOperator
case class Filter(val child: LogicalOperator,
                  val expression: Expression) extends LogicalOperator
case class InnerJoin(val left: LogicalOperator, val right: LogicalOperator,
                     val leftSelector: List[String], val rightSelector: List[String]) extends LogicalOperator
case class AntiJoin(val left: LogicalOperator, val right: LogicalOperator,
                    val leftSelector: List[String], val rightSelector: List[String]) extends LogicalOperator
case class Aggregate(val child: LogicalOperator,
                     val key: List[String],
                     val aggregates: List[AggregateFunction]) extends LogicalOperator
case class UnionAll(val children: List[LogicalOperator]) extends LogicalOperator
case class Project(val child: LogicalOperator,
                   val projector: List[String]) extends LogicalOperator
case class Compute(val child: LogicalOperator,
                   val aliasedExpressions: List[Tuple2[String, Expression]]) extends LogicalOperator           
case class With(val identifier: String,
                val withQuery: LogicalOperator,
                val childQuery: LogicalOperator) extends LogicalOperator
case class Reuse(val identifier: String) extends LogicalOperator
case class Benchmark(val identifier: String,
                     val child: LogicalOperator) extends LogicalOperator
case class PrintBenchmark(val identifier: String,
                          val child: LogicalOperator) extends LogicalOperator
case class Print(val child: LogicalOperator) extends LogicalOperator
case class Histogram(val identifier: String,
                     val child: LogicalOperator) extends LogicalOperator
case class HistogramFill(val identifier: String,
                         val child: LogicalOperator) extends LogicalOperator

case class QueueInit(val identifier: String,
                     val child: LogicalOperator) extends LogicalOperator
case class QueuePush(val identifier: String,
                     val child: LogicalOperator) extends LogicalOperator
case class QueuePop(val identifier: String) extends LogicalOperator

case class UDFFilter(val child: LogicalOperator) extends LogicalOperator