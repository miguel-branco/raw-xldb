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

import scala.collection.mutable.ListBuffer

abstract class Emitter {
  def emit(instruction: Instruction)
  def emit(instructions: List[Instruction])
  def generate()
}

/** Emit C++ code.
 *  Each instruction matches a C macro with the same name & arguments.
 *  Therefore, code is emitted as a sequence of Strings, each with a C macro call.
 */
class DefaultEmitter extends Emitter {
  private val code = new ListBuffer[String]
  
  def emit(instruction: Instruction) =
    code += (instruction.name + "(" + instruction.arguments.mkString(",") + ")")
  
  def emit(instructions: List[Instruction]) =
    for (instruction <- instructions) {
      emit(instruction)
    }

  def generate() = {
    val template = io.Source.fromFile("src/raw/code/executor/main-template.cc").mkString
    val sourceCode = template.replace("$MAIN$", code.mkString("\n  "))
    val output = new java.io.PrintWriter(new java.io.File("/home/miguel/test/query.cc"))
    try {
      output.write(sourceCode)
    } finally {
      output.close()
    }
  }
}