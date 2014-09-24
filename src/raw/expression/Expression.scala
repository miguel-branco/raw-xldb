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

import raw.schema.Schema
import raw.schema.DataType
import raw.schema.BOOL
import raw.schema.INT32
import raw.schema.FLOAT

sealed abstract class Expression {
  /** Return list of field names used within an expression. */
  def referencedFields: List[String] = 
    this match {
      case FieldReference(name) => List(name)
      case unaryOp : UnaryExpression => unaryOp.child.referencedFields
      case binaryOp : BinaryExpression => binaryOp.left.referencedFields ++ binaryOp.right.referencedFields
      case ternaryOp : TernaryExpression => ternaryOp.first.referencedFields ++ ternaryOp.second.referencedFields ++ ternaryOp.third.referencedFields   
      case _ => List()
    }
  
  /** Return data type of expression. If it is a FieldReference, consults schema passed as input. */
  def dataType(schema: Schema): DataType =
    this match {
      // FIXME: The following is error-prone and does no validation.
      case FieldReference(name) => schema.getField(name).dataType 
      case ConstFloat(_) => FLOAT
      case ConstInt32(_) => INT32
      case unaryOp : UnaryExpression => unaryOp.child.dataType(schema)
      // FIXME: "Trait" the expressions below so that I can handle them as I did for unaryOp above.
      case Plus(left, _) => left.dataType(schema)
      case Minus(left, _) => left.dataType(schema)
      case Multiply(left, _) => left.dataType(schema)
      case Divide(left, _) => left.dataType(schema)
      case And(_, _) => BOOL
      case Or(_, _) => BOOL
      case Equal(_, _) => BOOL
      case NotEqual(_, _) => BOOL
      case Less(_, _) => BOOL
      case LessOrEqual(_, _) => BOOL
      case Greater(_, _) => BOOL
      case GreaterOrEqual(_, _) => BOOL
      case If(_, second, _) => second.dataType(schema)
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
case class Cos(val child: Expression) extends Expression with UnaryExpression
case class Sin(val child: Expression) extends Expression with UnaryExpression
case class Sinh(val child: Expression) extends Expression with UnaryExpression
case class Sqrt(val child: Expression) extends Expression with UnaryExpression
case class Negate(val child: Expression) extends Expression with UnaryExpression

trait BinaryExpression {
  val left: Expression
  val right: Expression
}
case class Plus(left: Expression, right: Expression) extends Expression with BinaryExpression
case class Minus(left: Expression, right: Expression) extends Expression with BinaryExpression
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

trait TernaryExpression {
  val first: Expression
  val second: Expression
  val third: Expression
}
case class If(first: Expression, second: Expression, third: Expression) extends Expression with TernaryExpression
