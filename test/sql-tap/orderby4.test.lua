#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(6)

--!./tcltestrunner.lua
-- 2013 March 26
--
-- The author disclaims copyright to this source code.  In place of
-- a legal notice, here is a blessing:
--
--    May you do good and not evil.
--    May you find forgiveness for yourself and forgive others.
--    May you share freely, never taking more than you give.
--
-------------------------------------------------------------------------
-- This file implements regression tests for sql library.  The
-- focus of this file is testing that the optimizations that disable
-- ORDER BY clauses work correctly on multi-value primary keys and
-- unique indices when only some prefix of the terms in the key are
-- used.  See ticket http://www.sql.org/src/info/a179fe74659
--
-- ["set","testdir",[["file","dirname",["argv0"]]]]
-- ["source",[["testdir"],"\/tester.tcl"]]
testprefix = "orderby4"
-- Generate test data for a join.  Verify that the join gets the
-- correct answer.
--
test:do_execsql_test(
    "1.1",
    [[
        CREATE TABLE t1(a INT, b INT, PRIMARY KEY(a,b));
        INSERT INTO t1 VALUES(1,1),(1,2);
        CREATE TABLE t2(x INT, y INT, PRIMARY KEY(x,y));
        INSERT INTO t2 VALUES(3,3),(4,4);
        SELECT a, x FROM t1, t2 ORDER BY 1, 2;
    ]], {
        -- <1.1>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </1.1>
    })

test:do_execsql_test(
    "1.2",
    [[
        SELECT a, x FROM t1 CROSS JOIN t2 ORDER BY 1, 2;
    ]], {
        -- <1.2>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </1.2>
    })

test:do_execsql_test(
    "1.3",
    [[
        SELECT a, x FROM t2 CROSS JOIN t1 ORDER BY 1, 2;
    ]], {
        -- <1.3>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </1.3>
    })

test:do_execsql_test(
    "2.1",
    [[
        CREATE TABLE t3(id INT primary key, a INT);
        INSERT INTO t3 VALUES(1, 1),(2, 1);
        CREATE INDEX t3a ON t3(a);
        CREATE TABLE t4(id INT primary key, x INT);
        INSERT INTO t4 VALUES(1, 3),(2, 4);
        CREATE INDEX t4x ON t4(x);
        SELECT a, x FROM t3, t4 ORDER BY 1, 2;
    ]], {
        -- <2.1>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </2.1>
    })

test:do_execsql_test(
    "2.2",
    [[
        SELECT a, x FROM t3 CROSS JOIN t4 ORDER BY 1, 2;
    ]], {
        -- <2.2>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </2.2>
    })

test:do_execsql_test(
    "2.3",
    [[
        SELECT a, x FROM t4 CROSS JOIN t3 ORDER BY 1, 2;
    ]], {
        -- <2.3>
        1, 3, 1, 3, 1, 4, 1, 4
        -- </2.3>
    })

test:finish_test()

