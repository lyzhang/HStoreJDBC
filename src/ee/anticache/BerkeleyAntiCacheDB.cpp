#include "anticache/AntiCacheDB.h"
#include "anticache/BerkeleyAntiCacheDB.h"
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

#define ANTICACHE_DB_NAME "anticache.db" // dis gotta go

using namespace std;

namespace voltdb {

BerkeleyAntiCacheBlock::BerkeleyAntiCacheBlock(int16_t blockId, Dbt value) :
   AntiCacheBlock(blockId) 
    {
    m_buf = (char *) value.get_data();
    int16_t id = *((int16_t *)m_buf);
    long bufLen_ = sizeof(int16_t);
    std::string tableName = m_buf + bufLen_;
    bufLen_ += tableName.size()+1;
    long size = *((long *)(m_buf+bufLen_));
    bufLen_+=sizeof(long);
    char * data = m_buf + bufLen_;
    bufLen_ += size;
        
    payload p;
    p.blockId = id;
    p.tableName = tableName;
    p.data = data;
    p.size = size;
    m_size = size;
    m_payload = p;
    
    m_block = m_payload.data;
	    
    VOLT_DEBUG("BerkeleyAntiCacheBlock #%d from table: %s [size=%ld / payload=%ld = '%s']",
              blockId, m_payload.tableName.c_str(), m_size, m_payload.size, m_payload.data);
    //VOLT_INFO("data from getBlock %s", getData());
    m_blockType = ANTICACHEDB_BERKELEY;
}

BerkeleyAntiCacheBlock::~BerkeleyAntiCacheBlock() {
    if(m_blockId > 0 && m_buf != NULL) {
        free(m_buf);
    }
}

BerkeleyDBBlock::~BerkeleyDBBlock() {
    delete [] serialized_data;
}

BerkeleyAntiCacheDB::BerkeleyAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize) :
    AntiCacheDB(ctx, db_dir, blockSize, maxSize) {

    m_dbType = ANTICACHEDB_BERKELEY;
    initializeDB();
}


void BerkeleyAntiCacheDB::initializeDB() {
    
        u_int32_t env_flags =
        DB_CREATE       | // Create the environment if it does not exist
//        DB_AUTO_COMMIT  | // Immediately commit every operation
        DB_INIT_MPOOL   | // Initialize the memory pool (in-memory cache)
//        DB_TXN_NOSYNC   | // Don't flush to disk every time, we will do that explicitly
//        DB_INIT_LOCK    | // concurrent data store
        DB_PRIVATE      |
        DB_THREAD       | // allow multiple threads
//        DB_INIT_TXN     |
        DB_DIRECT_DB;     // Use O_DIRECT

    try {
        // allocate and initialize Berkeley DB database env
        m_dbEnv = new DbEnv(0); 
        m_dbEnv->open(m_dbDir.c_str(), env_flags, 0); 
        VOLT_INFO("created BerkeleyDB: %s\n", m_dbDir.c_str());
        // allocate and initialize new Berkeley DB instance
        m_db = new Db(m_dbEnv, 0); 
        m_db->open(NULL, ANTICACHE_DB_NAME, NULL, DB_HASH, DB_CREATE, 0); 

    } catch (DbException &e) {
        VOLT_ERROR("Anti-Cache initialization error: %s", e.what());
        VOLT_ERROR("Failed to initialize anti-cache database in directory %s", m_dbDir.c_str());
        throwFatalException("Failed to initialize anti-cache database in directory %s: %s",
                            m_dbDir.c_str(), e.what());
    }
}

void BerkeleyAntiCacheDB::shutdownDB() {
    
    // NOTE: You have to close the database first before closing the environment
    try 
    {
        m_db->close(0);
        delete m_db;
    } catch (DbException &e) 
    {
        VOLT_ERROR("Anti-Cache database closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database: %s", e.what());
    }
    
    try 
    {
        m_dbEnv->close(0);
        delete m_dbEnv;
    } catch (DbException &e) 
    {
        VOLT_ERROR("Anti-Cache environment closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database environment: %s", e.what());
    }
}

BerkeleyAntiCacheDB::~BerkeleyAntiCacheDB() {
    shutdownDB();
}

void BerkeleyAntiCacheDB::writeBlock(const std::string tableName,
                             int16_t blockId,
                             const int tupleCount,
                             const char* data,
                             const long size) {


    Dbt key;
    key.set_data(&blockId);
    key.set_size(sizeof(blockId));


    char * databuf_ = new char [size+tableName.size() + 1+sizeof(blockId)+sizeof(size)];
    memset(databuf_, 0, size+tableName.size() + 1+sizeof(blockId)+sizeof(size));
    // Now pack the data into a single contiguous memory location
    // for storage.
    long bufLen_ = 0;
    long dataLen = 0;
    dataLen = sizeof(blockId);
    memcpy(databuf_, &blockId, dataLen);
    bufLen_ += dataLen;
    dataLen = tableName.size() + 1;
    memcpy(databuf_ + bufLen_, tableName.c_str(), dataLen);
    bufLen_ += dataLen;
    dataLen = sizeof(size);
    memcpy(databuf_ + bufLen_, &size, dataLen);
    bufLen_ += dataLen;
    dataLen = size;
    memcpy(databuf_ + bufLen_, data, dataLen);
    bufLen_ += dataLen;

    Dbt value;
    value.set_data(databuf_);
    value.set_size(static_cast<int32_t>(bufLen_));


    VOLT_INFO("Writing out a block #%d to anti-cache database [tuples=%d / size=%ld]",
               blockId, tupleCount, size);
    // TODO: Error checking
    m_db->put(NULL, &key, &value, 0);
    
    pushBlockLRU(blockId);

    delete [] databuf_;
}

AntiCacheBlock* BerkeleyAntiCacheDB::readBlock(int16_t blockId) {
    Dbt key;
    key.set_data(&blockId);
    key.set_size(sizeof(blockId));

    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    
    VOLT_INFO("Reading evicted block with id %d", blockId);
    
    int ret_value = m_db->get(NULL, &key, &value, 0);

    if (ret_value != 0) 
    {
        VOLT_ERROR("Invalid anti-cache blockId '%d'", blockId);
        throw UnknownBlockAccessException(blockId);
    }
    else 
    {
//        m_db->del(NULL, &key, 0);  // if we have this the benchmark won't end
        assert(value.get_data() != NULL);
    }
    
    AntiCacheBlock* block = new BerkeleyAntiCacheBlock(blockId, value);
    
    removeBlockLRU(blockId);
    /*uint16_t rm_block = removeBlockLRU(blockId);
    if (rm_block != blockId) {
        VOLT_ERROR("LRU rm_block id: %d  and blockId %d not equal!", rm_block, blockId);
    }
 */
    return (block);
}

void BerkeleyAntiCacheDB::flushBlocks() {
    m_db->sync(0);
}

}
