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

#include <sstream>
#include <stdexcept>
#include "materializenode.h"
#include "common/common.h"
#include "common/serializeio.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "storage/table.h"

namespace voltdb {

MaterializePlanNode::~MaterializePlanNode() {
    delete getOutputTable();
    setOutputTable(NULL);
}

std::string MaterializePlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << this->ProjectionPlanNode::debugInfo(spacer);
    buffer << spacer << "batched: " << (this->batched ? "true" : "false") << "\n";
    return (buffer.str());
}

void MaterializePlanNode::loadFromJSONObject(json_spirit::Object &obj, const catalog::Database *catalog_db) {
    ProjectionPlanNode::loadFromJSONObject( obj, catalog_db);
    json_spirit::Value batchedValue = json_spirit::find_value( obj, "BATCHED");
    if (batchedValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "MaterializePlanNode::loadFromJSONObject:"
                                      " Can't find BATCHED value");
    }
    batched = batchedValue.get_bool();
}

}
