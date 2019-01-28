test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

function replace_id_by_name(r) r[1] = box.space[r[1]].name r[2] = box.space[r[1]].index[r[2]].name end
function show_names(t) for k,v in pairs(t) do if k > 0 then replace_id_by_name(v) end end return t end

-- Initializing some things.
box.sql.execute("CREATE TABLE t1(id INT PRIMARY KEY, a INT);")
box.sql.execute("CREATE TABLE t2(id INT PRIMARY KEY, a INT);")
box.sql.execute("CREATE INDEX i1 ON t1(a);")
box.sql.execute("CREATE INDEX i1 ON t2(a);")
box.sql.execute("INSERT INTO t1 VALUES(1, 2);")
box.sql.execute("INSERT INTO t2 VALUES(1, 2);")

-- Analyze.
box.sql.execute("ANALYZE;")

-- Checking the data.
show_names(box.sql.execute("SELECT * FROM \"_sql_stat\";"))

-- Dropping an index.
box.sql.execute("DROP INDEX i1 ON t1;")

-- Checking the DROP INDEX results.
show_names(box.sql.execute("SELECT * FROM \"_sql_stat\";"))

--Cleaning up.
box.sql.execute("DROP TABLE t1;")
box.sql.execute("DROP TABLE t2;")

-- Same test but dropping an INDEX ON t2.

box.sql.execute("CREATE TABLE t1(id INT PRIMARY KEY, a INT);")
box.sql.execute("CREATE TABLE t2(id INT PRIMARY KEY, a INT);")
box.sql.execute("CREATE INDEX i1 ON t1(a);")
box.sql.execute("CREATE INDEX i1 ON t2(a);")
box.sql.execute("INSERT INTO t1 VALUES(1, 2);")
box.sql.execute("INSERT INTO t2 VALUES(1, 2);")

-- Analyze.
box.sql.execute("ANALYZE;")

-- Checking the data.
show_names(box.sql.execute("SELECT * FROM \"_sql_stat\";"))
-- Dropping an index.
box.sql.execute("DROP INDEX i1 ON t2;")

-- Checking the DROP INDEX results.
show_names(box.sql.execute("SELECT * FROM \"_sql_stat\";"))
--Cleaning up.
box.sql.execute("DROP TABLE t1;")
box.sql.execute("DROP TABLE t2;")
