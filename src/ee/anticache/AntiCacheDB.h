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

#ifndef HSTOREANTICACHE_H
#define HSTOREANTICACHE_H

#include <db_cxx.h>

#include "common/debuglog.h"
#include "common/types.h"
#include "common/DefaultTupleSerializer.h"
#include "anticache/UnknownBlockAccessException.h"
#include "anticache/FullBackingStoreException.h"

#include <deque>
#include <map>
#include <vector>


//#define ANTICACHE_DB_NAME "anticache.db"

using namespace std;

namespace voltdb {
    
class ExecutorContext;
class AntiCacheDB;

/**
 * Wrapper class for an evicted block that has been read back in 
 * from the AntiCacheDB
 */
class AntiCacheBlock {
    friend class AntiCacheDB;
    
    public:
        virtual ~AntiCacheBlock() {};
        
        inline int16_t getBlockId() const {
            return m_blockId;
        }

        inline std::string getTableName() const {
            return (m_payload.tableName);
        }

        inline long getSize() const {
            return m_size;
        }
        inline char* getData() const {
            return m_block;
        }

        struct payload{
            int16_t blockId;
            std::string tableName;
            char * data;
            long size;
        };

        inline AntiCacheDBType getBlockType() const {
            return m_blockType;
        }
    
    protected:
        // Why is this private/protected?
        AntiCacheBlock(int16_t blockId);
        int16_t m_blockId;
        payload m_payload;
        long m_size;
        char * m_block;
        char * m_buf;
        // probably should be changed to a final/const
        AntiCacheDBType m_blockType;
}; // CLASS

class AntiCacheDB {
        
    public: 
       
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize);
        virtual ~AntiCacheDB();

        /**
         * Write a block of serialized tuples out to the anti-cache database
         */
        virtual void writeBlock(const std::string tableName,
                                int16_t blockId,
                                const int tupleCount,
                                const char* data,
                                const long size) = 0;
        /**
         * Read a block and return its contents
         */
        virtual AntiCacheBlock* readBlock(int16_t blockId) = 0;


        /**
         * Flush the buffered blocks to disk.
         */
        virtual void flushBlocks() = 0;

        /**
         * Return the next BlockId to use in the anti-cache database
         */
        virtual int16_t nextBlockId() = 0;
        /**
         * Return the AntiCacheDBType of the database
         */
        inline AntiCacheDBType getDBType() {
            return m_dbType;
        }
        /**
         * Return the blockSize of stored blocks
         */
        inline long getBlockSize() {
            return m_blockSize;
        }
        /**
         * Return the number of blocks stored in the database
         */
        inline int getNumBlocks() {
            return m_totalBlocks;
        }
        /**
         * Return the maximum size of the database
         */
        inline long getMaxDBSize() {
            return m_maxDBSize;
        }
        /**
         * Return the maximum number of blocks that can be stored 
         * in the database.
         */
        inline int getMaxBlocks() {
            return (int)(m_maxDBSize/m_blockSize);
        }
        /**
         * Return the number of free (available) blocks
         */
        inline int getFreeBlocks() {
            return getMaxBlocks()-getNumBlocks();
        }
        /**
         * Return the LRU block from the database. This *removes* the block
         * from the database. If you take it, it's yours, it exists nowhere else.
         * If a migrate or merge fails, you have to write it back. 
         *
         * It also updates removes the blockId from the LRU deque.
         */
        AntiCacheBlock* getLRUBlock();       

        /**
         * Removes a blockId from the LRU queue. This is used when reading a
         * specific block.
         */
        void removeBlockLRU(uint16_t blockId);
        
        /** 
         * Adds a blockId to the LRU deque.
         */
        void pushBlockLRU(uint16_t blockId);

        /**
         * Pops and returns the LRU blockID from the deque. This isn't a 
         * peek. When this function finishes, the block is in the database
         * but the blockId is no longer in the LRU. This shouldn't necessarily
         * be fatal, but it should be avoided.
         */
        inline uint16_t popBlockLRU();

        /**
         * Set the AntiCacheID number. This should be done on initialization and
         * should also match the the level in VoltDBEngine/executorcontext
         */
        inline void setACID(int16_t ACID) {
            m_ACID = ACID;
        }
        
        /**
         * Return the AntiCacheID number.
         */
        inline int16_t getACID() {
            return m_ACID;
        }

        /**
         * return true if we should stall false if we need to abort
         */
        inline bool stallForData() {
            return m_stall;
        }


    protected:
        ExecutorContext *m_executorContext;
        string m_dbDir;

        int16_t m_nextBlockId;
        int16_t m_ACID;
        long m_blockSize;
        int m_partitionId; 
        int m_totalBlocks; 
        
        bool m_stall;

        AntiCacheDBType m_dbType;
        long m_maxDBSize;

        /* we need to test whether a deque or list is better. If we push/pop more than we
         * remove, this is better. otherwise, let's use a list
         */

        std::deque<uint16_t> m_block_lru;

        /*
         * DB specific method of shutting down the database on destructor call
         */
        virtual void shutdownDB() = 0;

        
}; // CLASS

}
#endif
