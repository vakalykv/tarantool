#!/usr/bin/env tarantool
-- The maximum number of columns in sql is 2000. This test checks it.

test = require("sqltester")
test:plan(2)

success_column_count = 2000
fail_column_count = 2001

test:execsql("DROP TABLE IF EXISTS t1")
test:execsql("DROP TABLE IF EXISts t2")

local function form_create_statement(name, column_count)
	create_statement = 'CREATE TABLE ' .. name .. '('
	for i = 1,column_count do
		if i > 1 then create_statement = create_statement .. ',' end
		create_statement = create_statement .. 's' .. i .. ' INT'
		if i == 1 then create_statement = create_statement .. ' PRIMARY KEY' end
	end
	create_statement = create_statement .. ')'
	return create_statement
end

success_statement = form_create_statement("t1", success_column_count)
fail_statement = form_create_statement("t2", fail_column_count)

test:do_execsql_test(
	"columns-1.1",
	success_statement, {
		-- <columns-1.1>
		-- <columns-1.1>
	})

test:do_catchsql_test(
	"columns-1.2",
	fail_statement, {
		-- <columns-1.2>
		1, "too many columns on T2"
		-- <columns-1.2>
	})

test:finish_test()
