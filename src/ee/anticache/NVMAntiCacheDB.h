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

#ifndef NVMHSTOREANTICACHE_H
#define NVMHSTOREANTICACHE_H

#include "common/types.h"
#include "common/debuglog.h"
#include "anticache/AntiCacheDB.h"

using namespace std;

namespace voltdb {

class ExecutorContext;
class AntiCacheDB;

class NVMAntiCacheBlock : public AntiCacheBlock {
    friend class NVMAntiCacheDB;
    friend class AntiCacheDB;

    public:
        ~NVMAntiCacheBlock();

    private:
        NVMAntiCacheBlock(int16_t blockId, char* block, long size);
        //std::string m_tableName;
}; // CLASS

class NVMAntiCacheDB : public AntiCacheDB {
    public:
        NVMAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize);
        ~NVMAntiCacheDB();

        void initializeDB();

        inline int16_t nextBlockId() {
            return (int16_t)getFreeNVMBlockIndex(); 
        }

        AntiCacheBlock* readBlock(int16_t blockId);

        void shutdownDB();

        void flushBlocks();

        void writeBlock(const std::string tableName,
                        int16_t blockId,
                        const int tupleCount,
                        const char* data,
                        const long size);

    private:
        /**
         * NVM constants
         */
        /* I don't think these are used anymore -MJG
        static const off_t NVM_FILE_SIZE = 1073741824/2; 
        static const int NVM_BLOCK_SIZE = 524288 + 1000; 
        static const int MMAP_PAGE_SIZE = 2 * 1024 * 1024; 
        */
        FILE* nvm_file;
        char* m_NVMBlocks; 
        int nvm_fd; 
        int m_blockIndex;

        int m_nextFreeBlock; 
        
        /**
         *  List of free block indexes before the end of the last allocated block.
         */
        std::vector<int> m_NVMBlockFreeList; 

        /*
         *  Maps a block id to a <index, size> pair
         */
        std::map<int16_t, pair<int, int32_t> > m_blockMap; 

        /**
         *   Returns a pointer to the start of the block at the specified index. 
         */
        char* getNVMBlock(int index); 

        /**
         *  Adds the index to the free block list. 
         */
        void freeNVMBlock(int index);

        /**
         *   Returns the index of a free slot in the NVM block array. 
         */
        int getFreeNVMBlockIndex(); 
};

}
#endif
