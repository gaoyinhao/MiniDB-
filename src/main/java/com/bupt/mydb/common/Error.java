package com.bupt.mydb.common;

/**
 * @author gao98
 * date 2025/9/4 21:40
 * description:
 */
public class Error {

    //common
    public static final Exception CACHE_FULL_EXCEPTION = new RuntimeException("Cache is full");
    public static final Exception FILE_CAN_NOT_RW_EXCEPTION = new RuntimeException("File Can Not Read or Write");
    public static final Exception FILE_NOT_EXISTS_EXCEPTION = new RuntimeException("File does not Exists");
    public static final Exception FILE_EXISTS_EXCEPTION = new RuntimeException("File already exists");

    // dm
    public static final Exception BAD_LOG_FILE_EXCEPTION = new RuntimeException("Bad log file!");
    public static final Exception MEM_TOO_SMALL_EXCEPTION = new RuntimeException("Memory too small!");
    public static final Exception DATA_TOO_LARGE_EXCEPTION = new RuntimeException("Data too large!");
    public static final Exception DATABASE_BUSY_EXCEPTION = new RuntimeException("Database is busy!");


    //tm
    public static final Exception BADXID_FILE_EXCEPTION = new RuntimeException("Bad XID File");

    // vm
    public static final Exception DEADLOCK_EXCEPTION = new RuntimeException("Deadlock!");
    public static final Exception CONCURRENT_UPDATE_EXCEPTION = new RuntimeException("Concurrent update issue!");
    public static final Exception NULL_ENTRY_EXCEPTION = new RuntimeException("Null entry!");

    // tbm
    public static final Exception INVALID_FIELD_EXCEPTION = new RuntimeException("Invalid field type!");
    public static final Exception FIELD_NOT_FOUND_EXCEPTION = new RuntimeException("Field not found!");
    public static final Exception FIELD_NOT_INDEXED_EXCEPTION = new RuntimeException("Field not indexed!");
    public static final Exception INVALID_LOG_OP_EXCEPTION = new RuntimeException("Invalid logic operation!");
    public static final Exception INVALID_VALUES_EXCEPTION = new RuntimeException("Invalid values!");
    public static final Exception DUPLICATED_TABLE_EXCEPTION = new RuntimeException("Duplicated table!");
    public static final Exception TABLE_NOT_FOUND_EXCEPTION = new RuntimeException("Table not found!");

    // parser
    public static final Exception INVALID_COMMAND_EXCEPTION = new RuntimeException("Invalid command!");
    public static final Exception TABLE_NO_INDEX_EXCEPTION = new RuntimeException("Table has no index!");

    // transport
    public static final Exception INVALID_PKG_DATA_EXCEPTION = new RuntimeException("Invalid package data!");

    // server
    public static final Exception NESTED_TRANSACTION_EXCEPTION = new RuntimeException("Nested transaction not supported!");
    public static final Exception NO_TRANSACTION_EXCEPTION = new RuntimeException("Not in transaction!");

    // launcher
    public static final Exception INVALID_MEM_EXCEPTION = new RuntimeException("Invalid memory!");
}
