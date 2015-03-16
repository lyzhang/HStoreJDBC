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

#ifndef UNKNOWNBLOCKACCESSEXCEPTION_H_
#define UNKNOWNBLOCKACCESSEXCEPTION_H_

#include <stdint.h>
#include <string>
#include "common/SerializableEEException.h"

namespace voltdb {
class ReferenceSerializeOutput;

class UnknownBlockAccessException : public SerializableEEException {
    public:

        UnknownBlockAccessException(std::string tableName, uint16_t blockId);
        UnknownBlockAccessException(uint16_t blockId);
        virtual ~UnknownBlockAccessException() {}
        
        static std::string ERROR_MSG;
        
    protected:
        void p_serialize(ReferenceSerializeOutput *output);
        
    private:
        const std::string m_tableName;
        const uint16_t m_blockId;
};
}

#endif /* UNKNOWNBLOCKACCESSEXCEPTION_H_ */
