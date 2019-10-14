/****************************************************************

   main.c
     Copyright 2019.10.12 konoar

 ****************************************************************/
#include "libpq-fe.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define KS_OK               0
#define KS_NG               1

#define KS_PSTMT1_NAME      "P_INSERT_TEST"
#define KS_PSTMT2_NAME      "P_UPDATE_TEST"
#define KS_PSTMT3_NAME      "P_SELECT_TEST"

/*
 * SELECT oid, typarray, typname FROM pg_type;
 */
#define KS_TYPE_TIMESTAMP   1114
#define KS_TYPE_INT4        23
#define KS_TYPE_CHAR        1042
#define KS_TYPE_VARCHAR     1043

int ksMakeDate(char *buff, int offset)
{

    time_t t;
    struct tm *lt;

    time(&t);

    t += offset * 60 * 60 * 24;

    if (NULL == (lt = localtime(&t))) {
        return KS_NG;
    }

    sprintf(buff, "%04d-%02d-%02d 00:00:00",
        lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday);

    return KS_OK;

}

int ksPrepare(PGconn *conn)
{

    PGresult *res = NULL;
    int retval = KS_OK;

    do {

        res = PQprepare(

            conn, KS_PSTMT1_NAME,

            "INSERT INTO TestPQ.R_TEST(value, ctime) "
                "VALUES($1, $2)",

            2, (Oid []) {
                KS_TYPE_VARCHAR,
                KS_TYPE_TIMESTAMP
            }

        );

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);

        res = PQprepare(

            conn, KS_PSTMT2_NAME,

            "SELECT TestPQ.F_UPDATE_TEST($1, $2)",

            2,

            (Oid []) {
                KS_TYPE_CHAR,
                KS_TYPE_VARCHAR
            }

        );

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
            break;
        }

        PQclear(res);

        res = PQprepare(

            conn, KS_PSTMT3_NAME,

            "DECLARE mycursor CURSOR FOR "
                "SELECT "
                    "rid,    "
                    "tid,    "
                    "version,"
                    "value   "
                "FROM "
                    "TestPQ.F_SELECT_TEST($1)",

            1, (Oid []) {
                KS_TYPE_TIMESTAMP
            }

        );

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            retval = KS_NG;
        }

    } while(0);

    if (res) {
        PQclear(res);
        res = NULL;
    }

    return retval;

}

int ksInsert(PGconn *conn, const char *value)
{

    PGresult *res = NULL;
    int ret = KS_OK;
    char buff[32];

    if (KS_OK != (ret = ksMakeDate(buff, 0))) {
        return ret;
    }

    res = PQexecPrepared(

        conn, KS_PSTMT1_NAME,

        2, (const char* []) {
            value,
            buff
        },

        NULL, NULL, 0

    );

    if (res) {
        PQclear(res);
    }

    return KS_OK;

}

int ksUpdate(PGconn *conn, const char *tid, const char *value)
{

    PGresult *res = NULL;

    res = PQexecPrepared(

        conn, KS_PSTMT2_NAME,

        2, (const char* []) {
            tid,
            value
        },

        NULL, NULL, 0

    );

    if (res) {
        PQclear(res);
    }

    return KS_OK;

}

 int ksSelect(PGconn *conn, const char *tdate)
 {

    PGresult *res = NULL;

    putc('\n', stdout);

    res = PQexecPrepared(

        conn, KS_PSTMT3_NAME,

        1, (const char* []) {
            tdate
        },

        NULL, NULL, 0

    );

    PQclear(res);
    res = PQexec(conn, "FETCH FIRST IN mycursor");

    while (PGRES_TUPLES_OK == PQresultStatus(res) && 1 == PQntuples(res)) {

        printf("%-10s%-16s%-8s%-16s\n",
            PQgetvalue(res, 0, 0),
            PQgetvalue(res, 0, 1),
            PQgetvalue(res, 0, 2),
            PQgetvalue(res, 0, 3));

        PQclear(res);
        res = PQexec(conn, "FETCH NEXT IN mycursor");

    }

    if (res) {
        PQclear(res);
        res = NULL;
    }

    res = PQexec(conn, "CLOSE mycursor");

    if (res) {
        PQclear(res);
        res = NULL;
    }

    putc('\n', stdout);

    return KS_OK;

 }

int main(int argc, const  char* argv[])
{

    int ret = KS_OK;
    char buff[256];

    PGconn *conn;
    PGresult *res = NULL;

    sprintf(
        buff, "postgresql://%s@localhost:5432/%s",
        getenv("USER"), getenv("USER"));

    conn = PQconnectdb(buff);

    if (CONNECTION_OK != PQstatus(conn)) {
        return KS_NG;
    }

    do {

        // BEGIN TRANSACTION

        res = PQexec(conn,
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            ret = KS_NG;
            break;
        }

        PQclear(res);
        res = NULL;

        // PREPARE

        if (KS_OK != (ret = ksPrepare(conn))) {
            break;
        }

        // INSERT

        if (KS_OK != (ret = ksInsert(conn, "EEE"))) {
            break;
        }

        // UPDATE

        if (KS_OK != (ret = ksUpdate(conn, "TID00005", "eee"))) {
            break;
        }

        // SELECT

        for (int idx = 0; idx < 3; idx++) {

            ksMakeDate(buff, idx - 2);

            if (KS_OK != (ret = ksSelect(conn, buff))) {
                goto LFINISH;
            }

        }

        // END TRANSACTION

        res = PQexec(conn, "COMMIT");

        if (PGRES_COMMAND_OK != PQresultStatus(res)) {
            ret = KS_NG;
            break;
        }

    } while(0);

LFINISH:
    if (res) {
        PQclear(res);
        res = NULL;
    }

    PQfinish(conn);

    return ret;

}
