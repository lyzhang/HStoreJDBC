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
#include "anticache/NVMAntiCacheDB.h"
#include "anticache/UnknownBlockAccessException.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/executorcontext.hpp"
#include "common/types.h"
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>

using namespace std;

namespace voltdb {

NVMAntiCacheBlock::NVMAntiCacheBlock(int16_t blockId, char* block, long size) :
    AntiCacheBlock(blockId) {

    /* m_block = block;
    m_size = size;
    m_blockType = ANTICACHEDB_NVM;
    */
    char* buffer = block;
    std::string tableName = buffer;
    
    block = buffer + tableName.size() + 1;
    size -= tableName.size() + 1;


    m_block = new char[size];
    memcpy(m_block, block, size);

    delete [] buffer;

    payload p;
    p.tableName = tableName;
    p.blockId = blockId;
    p.data = m_block;
    p.size = size;
     
    m_payload = p;
    m_size = size;
    m_blockType = ANTICACHEDB_NVM;
    //std::string payload_str(m_payload.data, m_size);
    
    VOLT_INFO("NVMAntiCacheBlock #%d from table: %s [size=%ld / payload=%ld]",
              blockId, m_payload.tableName.c_str(), m_size, m_payload.size);
    
}

NVMAntiCacheBlock::~NVMAntiCacheBlock() {
    delete [] m_block;
}

NVMAntiCacheDB::NVMAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize) :
    AntiCacheDB(ctx, db_dir, blockSize, maxSize) {

    m_dbType = ANTICACHEDB_NVM;
    initializeDB();
}

NVMAntiCacheDB::~NVMAntiCacheDB() {
    shutdownDB();
}

void NVMAntiCacheDB::initializeDB() {
    
    char nvm_file_name[150];
    char partition_str[50];

    m_blockIndex = 0; 
    m_nextFreeBlock = 0;
    // TODO: Make DRAM based store a separate type
    #ifdef ANTICACHE_DRAM
        VOLT_INFO("Allocating anti-cache in DRAM."); 
        m_NVMBlocks = new char[m_maxDBSize];
    return; 
    #endif

    int partition_id;
    // use executor context to figure out which partition we are at
    // if there is no executor context, assume this is a test and let it go
    if (!m_executorContext) {
        VOLT_WARN("NVMAntiCacheDB has no executor context. If this is an EE test, don't worry\n");
        partition_id = 0;
    } else {
        partition_id = (int)m_executorContext->getPartitionId(); 
    }
    
    sprintf(partition_str, "%d", partition_id); 

    strcpy(nvm_file_name, m_dbDir.c_str()); 
    // there will be one NVM anti-cache file per partition, saved in /mnt/pmfs/anticache-XX
    strcat(nvm_file_name, "/anticache-");
    strcat(nvm_file_name, partition_str);
    VOLT_INFO("Creating nvm file: %s", nvm_file_name); 
    nvm_file = fopen(nvm_file_name, "w"); 

    if(nvm_file == NULL)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to open PMFS file %s: %s.", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    fclose(nvm_file); 
    nvm_file = fopen(nvm_file_name, "rw+"); 

    if(nvm_file == NULL)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to open PMFS file %s: %s.", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    nvm_fd = fileno(nvm_file); 
    if(nvm_fd < 0)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to allocate anti-cache PMFS file in directory %s.", m_dbDir.c_str());
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }
    
    if(ftruncate(nvm_fd, m_maxDBSize) < 0)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to ftruncate anti-cache PMFS file %s: %s", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    //off_t aligned_file_size = (((NVM_FILE_SIZE) + MMAP_PAGE_SIZE - 1) / MMAP_PAGE_SIZE * MMAP_PAGE_SIZE);  
    off_t aligned_file_size = (off_t)m_maxDBSize; 

    m_NVMBlocks =  (char*)mmap(NULL, aligned_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, nvm_fd, 0);
 
    if(m_NVMBlocks == MAP_FAILED)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to mmap PMFS file %s: %s", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    close(nvm_fd); // can safely close file now, mmap creates new reference
    
    // write out NULL characters to ensure entire file has been fetchted from memory
    
    for(int i = 0; i < m_maxDBSize; i++)
    {
        m_NVMBlocks[i] = '\0'; 
    }
    
}
void NVMAntiCacheDB::shutdownDB() { 
  fclose(nvm_file);
  #ifdef ANTICACHE_DRAM 
      delete [] m_NVMBlocks;
  #endif 
}

void NVMAntiCacheDB::flushBlocks() {
    //MJG TODO: Do we need a sync() or something?
}

void NVMAntiCacheDB::writeBlock(const std::string tableName,
                                int16_t blockId,
                                const int tupleCount,
                                const char* data,
                                const long size)  {
   
    if (getFreeBlocks() == 0) {
        VOLT_WARN("No free space in ACID %d for blockid %d with blocksize %ld",
                m_ACID, blockId, size);
        throw FullBackingStoreException(((int32_t)m_ACID << 16) & blockId, 0);
    }
    int index = (int)blockId;
    VOLT_TRACE("block index: %d", index);
    char* block = getNVMBlock(index); 
    long bufsize; 
    char* buffer = new char [tableName.size() + 1 + size];
    memset(buffer, 0, tableName.size() + 1 + size);
    bufsize = tableName.size() + 1;
    memcpy(buffer, tableName.c_str(), bufsize);
    memcpy(buffer + bufsize, data, size);
    bufsize += size;
    memcpy(block, buffer, bufsize); 
    delete[] buffer;

    VOLT_INFO("Writing NVM Block: ID = %d, index = %d, size = %ld", blockId, index, bufsize); 

    m_blockMap.insert(std::pair<int16_t, std::pair<int, int32_t> >(blockId, std::pair<int, int32_t>(index, static_cast<int32_t>(bufsize))));
    m_nextFreeBlock++; 
    
    pushBlockLRU(blockId);
}

AntiCacheBlock* NVMAntiCacheDB::readBlock(int16_t blockId) {
    
    std::map<int16_t, std::pair<int, int32_t> >::iterator itr; 
    itr = m_blockMap.find(blockId); 
  
    if (itr == m_blockMap.end()) {
        VOLT_INFO("Invalid anti-cache blockId '%d'", blockId);
        VOLT_ERROR("Invalid anti-cache blockId '%d'", blockId);
        //throw UnknownBlockAccessException(tableName, blockId);
        throw UnknownBlockAccessException(blockId);
   
    }

    int blockIndex = itr->second.first; 
    int blockSize = itr->second.second;
    VOLT_INFO("Reading NVM block: ID = %d, index = %d, size = %d", blockId, blockIndex, blockSize);
   
    char* block_ptr = getNVMBlock(blockIndex);
    char* block = new char[blockSize];
    memcpy(block, block_ptr, blockSize); 
    
    AntiCacheBlock* anticache_block = new NVMAntiCacheBlock(blockId, block, blockSize);

    freeNVMBlock(blockId); 

    m_blockMap.erase(itr); 

    removeBlockLRU(blockId);
    return (anticache_block);
}

char* NVMAntiCacheDB::getNVMBlock(int index) {
    //char* nvm_block = new char[NVM_BLOCK_SIZE];     
    //memcpy(nvm_block, m_NVMBlocks+(index*NVM_BLOCK_SIZE), NVM_BLOCK_SIZE); 
    
    //return nvm_block; 
    //return (m_NVMBlocks+(index*NVM_BLOCK_SIZE));  
    return (m_NVMBlocks+(index*m_blockSize));
}

int NVMAntiCacheDB::getFreeNVMBlockIndex() {
  
    int free_index = 0; 
    if(m_NVMBlockFreeList.size() > 0) {
        free_index = m_NVMBlockFreeList.back(); 
        VOLT_TRACE("popping %d from list of size: %d", free_index, (int)m_NVMBlockFreeList.size());
        m_NVMBlockFreeList.pop_back(); 
    } else {
        if (m_nextFreeBlock == getMaxBlocks()) {
            throw FullBackingStoreException(0, m_nextFreeBlock);
        } else {
            free_index = m_nextFreeBlock;
        }
    }
  
    //int free_index = m_blockIndex++; 
    return free_index; 
}

void NVMAntiCacheDB::freeNVMBlock(int index) {
    m_NVMBlockFreeList.push_back(index); 
    VOLT_TRACE("list size: %d  back: %d", (int)m_NVMBlockFreeList.size(), m_NVMBlockFreeList.back());
    //m_blockIndex--; 
}
}
