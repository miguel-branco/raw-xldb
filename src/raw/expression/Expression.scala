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
package raw.expression

sealed class Expression {
  /** Return list of field names used within an expression. */
  def getFields: List[String] = 
      this match {
        case unaryOp : UnaryExpression => unaryOp.child.getFields
        case binaryOp : BinaryExpression => binaryOp.left.getFields ++ binaryOp.right.getFields
        case FieldReference(name) => List(name)
        case _ => List()
      }    
}

trait TerminalExpression
case class FieldReference(name: String) extends Expression with TerminalExpression
case class ConstFloat(val value: Double) extends Expression with TerminalExpression
case class ConstInt32(val value: Integer) extends Expression with TerminalExpression

trait UnaryExpression {
  val child: Expression
}
case class Abs(val child: Expression) extends Expression with UnaryExpression

trait BinaryExpression {
  val left: Expression
  val right: Expression
}
case class Plus(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Multiply(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Divide(left: Expression, right: Expression) extends Expression with BinaryExpression
case class And(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Or(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Equal(left: Expression, right: Expression) extends Expression with BinaryExpression
case class NotEqual(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Less(left: Expression, right: Expression) extends Expression with BinaryExpression
case class LessOrEqual(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Greater(left: Expression, right: Expression) extends Expression with BinaryExpression
case class GreaterOrEqual(left: Expression, right: Expression) extends Expression with BinaryExpression
