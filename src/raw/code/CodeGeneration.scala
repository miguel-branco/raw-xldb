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
package raw.code

import raw.code.executor.Executor

import raw.code.storage.AccessPath
import raw.code.storage.Storage

import raw.operators.PhysicalOperator
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

object CodeGeneration {
  def generate(queries: List[PhysicalOperator], emitter: Emitter, executor: Executor) = {
    emitter.emit(executor.init())
    for (query <- queries)
      emitter.emit(executor.code(query))
    emitter.emit(executor.done())
    emitter.generate()
  }
}