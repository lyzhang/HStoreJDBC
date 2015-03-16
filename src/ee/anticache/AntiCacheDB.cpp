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

#include "anticache/AntiCacheDB.h"
#include "anticache/UnknownBlockAccessException.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/executorcontext.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>

using namespace std;

namespace voltdb {

AntiCacheBlock::AntiCacheBlock(int16_t blockId) {

//    for(int i=0;i<size;i++){
//       VOLT_INFO("%x", data[i]);
//    }
        m_blockId = blockId;

  }
   
AntiCacheDB::AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize) :
    m_executorContext(ctx),
    m_dbDir(db_dir),
    m_nextBlockId(0),
    m_blockSize(blockSize),
    m_totalBlocks(0)
    { 
        // MJG: TODO: HACK: Come up with a better way to make a maxsize when one isn't given
        if (maxSize == -1) {
            m_maxDBSize = 1000*blockSize;
        } else {
            m_maxDBSize = maxSize;
        }
        
}

AntiCacheDB::~AntiCacheDB() {
}

AntiCacheBlock* AntiCacheDB::getLRUBlock() {
    uint16_t lru_block_id;
    AntiCacheBlock* lru_block;

    if (m_block_lru.empty()) {
        VOLT_ERROR("LRU Blocklist Empty!");
        throw UnknownBlockAccessException(0);
    } else {
        lru_block_id = m_block_lru.front();
        //m_block_lru.pop_front();
        lru_block = readBlock(lru_block_id);
        m_totalBlocks--;
        return lru_block;
    }
}

void AntiCacheDB::removeBlockLRU(uint16_t blockId) {
    std::deque<uint16_t>::iterator it;
    bool found = false;
           
    
    for (it = m_block_lru.begin(); it != m_block_lru.end(); ++it) {
        if (*it == blockId) {
            VOLT_INFO("Found block id %d == blockId %d", *it, blockId);
            m_block_lru.erase(it);
            found = true;
            m_totalBlocks--;
            break;
        }
    }

    if (!found) {
        VOLT_ERROR("Found block but didn't find blockId %d in LRU!", blockId);
        //throw UnknownBlockAccessException(blockId);
    }
}

void AntiCacheDB::pushBlockLRU(uint16_t blockId) {
    VOLT_INFO("Pushing blockId %d into LRU", blockId);
    m_block_lru.push_back(blockId);
    m_totalBlocks++;
}

uint16_t AntiCacheDB::popBlockLRU() {
    uint16_t blockId = m_block_lru.front();
    m_block_lru.pop_front();
    return blockId;
}


}

