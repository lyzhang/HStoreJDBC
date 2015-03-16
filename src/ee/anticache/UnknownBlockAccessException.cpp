/* Copyright (C) 2012 by H-Store Project
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

#include "anticache/UnknownBlockAccessException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>

using namespace voltdb;

std::string UnknownBlockAccessException::ERROR_MSG = std::string("Tried to access unknown block");

UnknownBlockAccessException::UnknownBlockAccessException(std::string tableName, uint16_t blockId) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_UNKNOWN_BLOCK, UnknownBlockAccessException::ERROR_MSG),
    	m_tableName(tableName),
        m_blockId(blockId) {
    
    // Nothing to see, nothing to do...
}

UnknownBlockAccessException::UnknownBlockAccessException(uint16_t blockId) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_UNKNOWN_BLOCK, UnknownBlockAccessException::ERROR_MSG),
        m_blockId(blockId) {
}

void UnknownBlockAccessException::p_serialize(ReferenceSerializeOutput *output) {
    if(!m_tableName.empty()){
	output->writeTextString(m_tableName);
    }
    output->writeShort(m_blockId);
}
