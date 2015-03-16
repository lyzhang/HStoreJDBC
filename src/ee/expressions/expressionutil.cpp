/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "expressionutil.h"

#include "common/debuglog.h"
#include "common/ValueFactory.hpp"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"

#include <cassert>
#include <sstream>
#include <cstdlib>
#include <stdexcept>

#include "json_spirit/json_spirit.h"

namespace voltdb {

/** Function static helper templated functions to vivify an optimal
    comparison class. */
AbstractExpression*
getGeneral(ExpressionType c,
           AbstractExpression *l,
           AbstractExpression *r)
{
    assert (l);
    assert (r);
    switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
        return new ComparisonExpression<CmpEq>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
        return new ComparisonExpression<CmpNe>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
        return new ComparisonExpression<CmpLt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
        return new ComparisonExpression<CmpGt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
        return new ComparisonExpression<CmpLte>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
        return new ComparisonExpression<CmpGte>(c, l, r);
    default:
        char message[256];
        sprintf(message, "Invalid ExpressionType '%s' called"
                " for ComparisonExpression",
                expressionutil::getTypeName(c).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
}


template <typename L, typename R>
AbstractExpression*
getMoreSpecialized(ExpressionType c, L* l, R* r)
{
    assert (l);
    assert (r);
    switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
        return new InlinedComparisonExpression<CmpEq, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
        return new InlinedComparisonExpression<CmpNe, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
        return new InlinedComparisonExpression<CmpLt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
        return new InlinedComparisonExpression<CmpGt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
        return new InlinedComparisonExpression<CmpLte, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
        return new InlinedComparisonExpression<CmpGte, L, R>(c, l, r);
    default:
        char message[256];
        sprintf(message, "Invalid ExpressionType '%s' called for"
                " ComparisonExpression", expressionutil::getTypeName(c).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
}

/** convert the enumerated value type into a concrete c type for the
 * comparison helper templates. */
AbstractExpression *
comparisonFactory(ExpressionType c,
                  AbstractExpression *lc, AbstractExpression *rc)
{
    assert(lc);
    /*printf("left: %s\n", left_optimized->debug("").c_str());
    fflush(stdout);
    printf("right: %s\n", right_optimized->debug("").c_str());
    fflush(stdout);*/

    //printf("%s\n", right_optimized->debug().c_str());
    //fflush(stdout);

    VOLT_TRACE("comparisonFactoryHelper request:%s left:%s(%p) right:%s(%p)",
               expressionutil::getTypeName(c).c_str(),
               typeid(*(lc)).name(), lc,
               typeid(*(rc)).name(), rc);

    /*if (!l) {
        printf("no left\n");
        fflush(stdout);
        exit(0);
    }*/

    // more specialization available?
    ConstantValueExpression *l_const =
      dynamic_cast<ConstantValueExpression*>(lc);

    ConstantValueExpression *r_const =
      dynamic_cast<ConstantValueExpression*>(rc);

    TupleValueExpression *l_tuple =
      dynamic_cast<TupleValueExpression*>(lc);

    TupleValueExpression *r_tuple =
      dynamic_cast<TupleValueExpression*>(rc);

    // this will inline getValue(), hooray!
    if (l_const != NULL && r_const != NULL) { // CONST-CONST can it happen?
        return getMoreSpecialized<ConstantValueExpression,
            ConstantValueExpression>(c, l_const, r_const);
    } else if (l_const != NULL && r_tuple != NULL) { // CONST-TUPLE
        return getMoreSpecialized<ConstantValueExpression,
          TupleValueExpression>(c, l_const, r_tuple);
    } else if (l_tuple != NULL && r_const != NULL) { // TUPLE-CONST
        return getMoreSpecialized<TupleValueExpression,
          ConstantValueExpression >(c, l_tuple, r_const);
    } else if (l_tuple != NULL && r_tuple != NULL) { // TUPLE-TUPLE
        return getMoreSpecialized<TupleValueExpression,
          TupleValueExpression>(c, l_tuple, r_tuple);
    }

    //okay, still getTypedValue is beneficial.
    return getGeneral(c, lc, rc);
}

/** convert the enumerated value type into a concrete c type for the
 *  operator expression templated ctors */
AbstractExpression *
operatorFactory(ExpressionType et,
                AbstractExpression *lc, AbstractExpression *rc)
{
    AbstractExpression *ret = NULL;

   switch(et) {
     case (EXPRESSION_TYPE_OPERATOR_PLUS):
       ret = new OperatorExpression<OpPlus>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_MINUS):
       ret = new OperatorExpression<OpMinus>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
       ret = new OperatorExpression<OpMultiply>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
       ret = new OperatorExpression<OpDivide>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_NOT):
       ret = new OperatorNotExpression(lc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_MOD):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Mod operator is not yet supported.");

     case (EXPRESSION_TYPE_OPERATOR_CONCAT):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Concat operator not yet supported.");

     case (EXPRESSION_TYPE_OPERATOR_CAST):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Cast operator not yet supported.");

     default:
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "operator ctor helper out of sync");
   }
   return ret;
}

/** convert the enumerated value type into a concrete c type for
 * constant value expressions templated ctors */
AbstractExpression*
constantValueFactory(json_spirit::Object &obj,
                     ValueType vt, ExpressionType et,
                     AbstractExpression *lc, AbstractExpression *rc)
{
    // read before ctor - can then instantiate fully init'd obj.
    NValue newvalue;
    json_spirit::Value valueValue = json_spirit::find_value( obj, "VALUE");
    if (valueValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Could not find"
                                      " VALUE value");
    }

    if (valueValue.type() == json_spirit::str_type)
    {
        std::string nullcheck = valueValue.get_str();
        if (nullcheck == "NULL")
        {
            newvalue = NValue::getNullValue(vt);
            return constantValueFactory(newvalue);
        }
    }

    switch (vt) {
    case VALUE_TYPE_INVALID:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Value type should"
                                      " never be VALUE_TYPE_INVALID");
    case VALUE_TYPE_NULL:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: And they should be"
                                      " never be this either! VALUE_TYPE_NULL");
    case VALUE_TYPE_TINYINT:
        newvalue = ValueFactory::getTinyIntValue(static_cast<int8_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_SMALLINT:
        newvalue = ValueFactory::getSmallIntValue(static_cast<int16_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_INTEGER:
        newvalue = ValueFactory::getIntegerValue(static_cast<int32_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_BIGINT:
        newvalue = ValueFactory::getBigIntValue(static_cast<int64_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_DOUBLE:
        newvalue = ValueFactory::getDoubleValue(static_cast<double>(valueValue.get_real()));
        break;
    case VALUE_TYPE_VARCHAR:
        newvalue = ValueFactory::getStringValue(valueValue.get_str());
        break;
    case VALUE_TYPE_VARBINARY:
		// uses hex encoding
		newvalue = ValueFactory::getBinaryValue(valueValue.get_str());
		break;
    case VALUE_TYPE_TIMESTAMP:
        newvalue = ValueFactory::getTimestampValue(static_cast<int64_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_DECIMAL:
        newvalue = ValueFactory::getDecimalValueFromString(valueValue.get_str());
        break;
    default:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Unrecognized value"
                                      " type");
    }

    return constantValueFactory(newvalue);
}

/** provide an interface for creating constant value expressions that
 * is more useful to testcases */
AbstractExpression *
constantValueFactory(const NValue &newvalue)
{
    return new ConstantValueExpression(newvalue);
 /*   switch (vt) {
        case (VALUE_TYPE_TINYINT):
        case (VALUE_TYPE_SMALLINT):
        case (VALUE_TYPE_INTEGER):
        case (VALUE_TYPE_TIMESTAMP):
        case (VALUE_TYPE_BIGINT):
            return new ConstantValueExpression<int64_t>(newvalue);
        case (VALUE_TYPE_DOUBLE):
            return new OptimizedConstantValueExpression<double>(newvalue);
        case (VALUE_TYPE_VARCHAR):
            return new OptimizedConstantValueExpression<VoltString>(newvalue);
        case (VALUE_TYPE_DECIMAL):
            return new OptimizedConstantValueExpression<VoltDecimal>(newvalue);
        default:
            VOLT_ERROR("unknown type '%d'", vt);
            assert (!"unknown type in constantValueFactory");
            return NULL;
    }*/
}

/** convert the enumerated value type into a concrete c type for
 * parameter value expression templated ctors */
AbstractExpression*
parameterValueFactory(json_spirit::Object &obj,
                      ExpressionType et,
                      AbstractExpression *lc, AbstractExpression *rc)
{
    // read before ctor - can then instantiate fully init'd obj.
    json_spirit::Value paramIdxValue = json_spirit::find_value( obj, "PARAM_IDX");
    if (paramIdxValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "parameterValueFactory: Could not find"
                                      " PARAM_IDX value");
    }
    int param_idx = paramIdxValue.get_int();
    assert (param_idx >= 0);
    return parameterValueFactory(param_idx);
}

AbstractExpression * parameterValueFactory(int idx) {
    return new ParameterValueExpression(idx);
}

/** convert the enumerated value type into a concrete c type for
 * tuple value expression templated ctors */
AbstractExpression*
tupleValueFactory(json_spirit::Object &obj, ExpressionType et,
                  AbstractExpression *lc, AbstractExpression *rc)
{
    // read the tuple value expression specific data
    json_spirit::Value valueIdxValue =
      json_spirit::find_value( obj, "COLUMN_IDX");

    json_spirit::Value tableName =
      json_spirit::find_value(obj, "TABLE_NAME");

    json_spirit::Value columnName =
      json_spirit::find_value(obj, "COLUMN_NAME");

    // verify input
    if (valueIdxValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: Could not find"
                                      " COLUMN_IDX value");
    }
    if (valueIdxValue.get_int() < 0) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: invalid column_idx.");
    }

    if (tableName == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: no table name in TVE");
    }

    if (columnName == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: no column name in"
                                      " TVE");
    }


    return new TupleValueExpression(valueIdxValue.get_int(),
                                    tableName.get_str(),
                                    columnName.get_str());
}

AbstractExpression *
conjunctionFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc)
{
    switch (et) {
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
        return new ConjunctionExpression<ConjunctionAnd>(et, lc, rc);
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
        return new ConjunctionExpression<ConjunctionOr>(et, lc, rc);
    default:
        return NULL;
    }

}


/** Given an expression type and a valuetype, find the best
 * templated ctor to invoke. Several helpers, above, aid in this
 * pursuit. Each instantiated expression must consume any
 * class-specific serialization from serialize_io. */

AbstractExpression*
expressionFactory(json_spirit::Object &obj,
                  ExpressionType et, ValueType vt, int vs,
                  AbstractExpression* lc,
                  AbstractExpression* rc)
{
    VOLT_TRACE("expressionFactory request: %s(%d), %s(%d), %d, left: %p"
               " right: %p",
               expressionutil::getTypeName(et).c_str(), et,
               getTypeName(vt).c_str(), vt, vs,
               lc, rc);

    AbstractExpression *ret = NULL;

    switch (et) {

        // Operators
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
    case (EXPRESSION_TYPE_OPERATOR_MINUS):
    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
    case (EXPRESSION_TYPE_OPERATOR_MOD):
    case (EXPRESSION_TYPE_OPERATOR_CAST):
    case (EXPRESSION_TYPE_OPERATOR_NOT):
        ret = operatorFactory(et, lc, rc);
    break;

    // Comparisons
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
        ret = comparisonFactory( et, lc, rc);
    break;

    // Conjunctions
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
        ret = conjunctionFactory(et, lc, rc);
    break;

    // Constant Values, parameters, tuples
    case (EXPRESSION_TYPE_VALUE_CONSTANT):
        ret = constantValueFactory(obj, vt, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_PARAMETER):
        ret = parameterValueFactory(obj, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_TUPLE):
        ret = tupleValueFactory(obj, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
        ret = new TupleAddressExpression();
        break;

        // must handle all known expressions in this factory
    default:
        char message[256];
        sprintf(message, "Invalid ExpressionType '%s' requested from factory",
                expressionutil::getTypeName(et).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    // written thusly to ease testing/inspecting return content.
    VOLT_TRACE("Created '%s' expression %p", expressionutil::getTypeName(et).c_str(), ret);
    return ret;
}

} // namespace voltdb
namespace expressionutil {

boost::shared_array<int>
convertIfAllTupleValues(const std::vector<voltdb::AbstractExpression*> &expressions)
{
    size_t cnt = expressions.size();
    boost::shared_array<int> ret(new int[cnt]);
    for (int i = 0; i < cnt; ++i) {
        voltdb::TupleValueExpressionMarker* casted=
          dynamic_cast<voltdb::TupleValueExpressionMarker*>(expressions[i]);
        if (casted == NULL) {
            return boost::shared_array<int>();
        }
        ret[i] = casted->getColumnId();
    }
    return ret;
}

boost::shared_array<int>
convertIfAllParameterValues(const std::vector<voltdb::AbstractExpression*> &expressions)
{
    size_t cnt = expressions.size();
    boost::shared_array<int> ret(new int[cnt]);
    for (int i = 0; i < cnt; ++i) {
        voltdb::ParameterValueExpressionMarker *casted =
          dynamic_cast<voltdb::ParameterValueExpressionMarker*>(expressions[i]);
        if (casted == NULL) {
            return boost::shared_array<int>();
        }
        ret[i] = casted->getParameterId();
    }
    return ret;
}


/** return a descriptive string for each typename. could just
    as easily be a lookup table */
std::string
getTypeName(voltdb::ExpressionType type)
{
    std::string ret;
    switch (type) {
        case (voltdb::EXPRESSION_TYPE_OPERATOR_PLUS):
            ret = "OPERATOR_PLUS";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_MINUS):
            ret = "OPERATOR_MINUS";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_MULTIPLY):
            ret = "OPERATOR_MULTIPLY";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_DIVIDE):
            ret = "OPERATOR_DIVIDE";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_CONCAT):
            ret = "OPERATOR_CONCAT";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_MOD):
            ret = "OPERATOR_MOD";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_CAST):
            ret = "OPERATOR_CAST";
            break;
        case (voltdb::EXPRESSION_TYPE_OPERATOR_NOT):
            ret = "OPERATOR_NOT";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_EQUAL):
            ret = "COMPARE_EQUAL";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_NOTEQUAL):
            ret = "COMPARE_NOTEQUAL";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_LESSTHAN):
            ret = "COMPARE_LESSTHAN";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_GREATERTHAN):
            ret = "COMPARE_GREATERTHAN";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
            ret = "COMPARE_LESSTHANOREQUALTO";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
            ret = "COMPARE_GREATERTHANOREQUALTO";
            break;
        case (voltdb::EXPRESSION_TYPE_COMPARE_LIKE):
            ret = "COMPARE_LIKE";
            break;
        case (voltdb::EXPRESSION_TYPE_CONJUNCTION_AND):
            ret = "CONJUNCTION_AND";
            break;
        case (voltdb::EXPRESSION_TYPE_CONJUNCTION_OR):
            ret = "CONJUNCTION_OR";
            break;
        case (voltdb::EXPRESSION_TYPE_VALUE_CONSTANT):
            ret = "VALUE_CONSTANT";
            break;
        case (voltdb::EXPRESSION_TYPE_VALUE_PARAMETER):
            ret = "VALUE_PARAMETER";
            break;
        case (voltdb::EXPRESSION_TYPE_VALUE_TUPLE):
            ret = "VALUE_TUPLE";
            break;
        case (voltdb::EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
            ret = "VALUE_TUPLE_ADDRESS";
            break;
        case (voltdb::EXPRESSION_TYPE_VALUE_NULL):
            ret = "VALUE_NULL";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_COUNT):
            ret = "AGGREGATE_COUNT";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_COUNT_STAR):
            ret = "AGGREGATE_COUNT_STAR";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_SUM):
            ret = "AGGREGATE_SUM";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_MIN):
            ret = "AGGREGATE_MIN";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_MAX):
            ret = "AGGREGATE_MAX";
            break;
        case (voltdb::EXPRESSION_TYPE_AGGREGATE_AVG):
            ret = "AGGREGATE_AVG";
            break;
        case (voltdb::EXPRESSION_TYPE_INVALID):
            ret = "INVALID";
            break;
        default: {
            char buffer[32];
            sprintf(buffer, "UNKNOWN[%d]", type);
            ret = buffer;
        }
    }
    return (ret);
}

}
