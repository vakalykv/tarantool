remote = require('net.box')
test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

-- gh-3010: COLLATE after LIMIT should throw an error

-- All of these tests should throw error "near "COLLATE": syntax error"
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY OFFSET 1;")
box.sql.execute("SELECT 1 LIMIT 1 OFFSET 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1, 1 COLLATE BINARY;")
box.sql.execute("SELECT 1 LIMIT 1 COLLATE BINARY, 1;")

-- gh-3052: upper/lower support only default locale
-- For tr-TR result depends on collation
box.sql.execute([[CREATE TABLE tu (descriptor CHAR(50) PRIMARY KEY, letter CHAR(50))]]);
box.internal.collation.create('TURKISH', 'ICU', 'tr-TR', {strength='primary'});
box.sql.execute([[INSERT INTO tu VALUES ('Latin Capital Letter I U+0049','I');]])
box.sql.execute([[INSERT INTO tu VALUES ('Latin Small Letter I U+0069','i');]])
box.sql.execute([[INSERT INTO tu VALUES ('Latin Capital Letter I With Dot Above U+0130','İ');]])
box.sql.execute([[INSERT INTO tu VALUES ('Latin Small Letter Dotless I U+0131','ı');]])
-- Without collation
box.sql.execute([[SELECT descriptor, upper(letter) AS upper,lower(letter) AS lower FROM tu;]])
-- With collation
box.sql.execute([[SELECT descriptor, upper(letter COLLATE "TURKISH") AS upper,lower(letter COLLATE "TURKISH") AS lower FROM tu;]])
box.internal.collation.drop('TURKISH')

-- For de-DE result is actually the same
box.internal.collation.create('GERMAN', 'ICU', 'de-DE', {strength='primary'});
box.sql.execute([[INSERT INTO tu VALUES ('German Small Letter Sharp S U+00DF','ß');]])
-- Without collation
box.sql.execute([[SELECT descriptor, upper(letter), letter FROM tu where UPPER(letter) = 'SS';]])
-- With collation
box.sql.execute([[SELECT descriptor, upper(letter COLLATE "GERMAN"), letter FROM tu where UPPER(letter COLLATE "GERMAN") = 'SS';]])
box.internal.collation.drop('GERMAN')
box.sql.execute(([[DROP TABLE tu]]))

box.schema.user.grant('guest','read,write,execute', 'universe')
cn = remote.connect(box.cfg.listen)

cn:execute('select 1 limit ? collate not_exist', {1})

cn:close()

-- Explicitly set BINARY collation is predifined and has ID.
--
box.sql.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT COLLATE \"binary\");")
box.space.T:format()[2]['collation']
box.space.T:format()[3]['collation']
box.sql.execute("DROP TABLE t;")

-- BINARY collation works in the same way as there is no collation
-- at all.
--
t = box.schema.create_space('test', {format = {{'id', 'unsigned'}, {'a', 'string', collation = 'binary'}}})
t:format()[2]['collation']
pk = t:create_index('primary', {parts = {1}})
i = t:create_index('secondary', {parts = {2, 'str', collation='binary'}})
t:insert({1, 'AsD'})
t:insert({2, 'ASD'})
t:insert({3, 'asd'})
i:select('asd')
i:select('ASD')
i:select('AsD')
t:drop()

-- Collation with id == 0 is "none". It used to unify interaction
-- with collation interface. It also can't be dropped.
--
box.space._collation:select{0}
box.space._collation:delete{0}

-- gh-3185: collations of LHS and RHS must be compatible.
--
box.sql.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT COLLATE \"binary\", c TEXT COLLATE \"unicode_ci\");")
box.sql.execute("SELECT * FROM t WHERE a = b;")
box.sql.execute("SELECT * FROM t WHERE a COLLATE \"binary\" = b;")
box.sql.execute("SELECT * FROM t WHERE b = c;")
box.sql.execute("SELECT * FROM t WHERE b COLLATE \"binary\" = c;")
box.sql.execute("SELECT * FROM t WHERE a = c;")
box.sql.execute("SELECT * FROM t WHERE a COLLATE \"binary\" = c COLLATE \"unicode\";")

-- Compound queries perform implicit comparisons between values.
-- Hence, rules for collations compatibilities are the same.
--
box.sql.execute("SELECT 'abc' COLLATE \"binary\" UNION SELECT 'ABC' COLLATE \"unicode_ci\"")
box.sql.execute("SELECT 'abc' COLLATE \"unicode_ci\" UNION SELECT 'ABC' COLLATE binary")
box.sql.execute("SELECT c FROM t UNION SELECT b FROM t;")
box.sql.execute("SELECT b FROM t UNION SELECT a FROM t;")
box.sql.execute("SELECT a FROM t UNION SELECT c FROM t;")
box.sql.execute("SELECT c COLLATE \"binary\" FROM t UNION SELECT a FROM t;")
box.sql.execute("SELECT b COLLATE \"unicode\" FROM t UNION SELECT a FROM t;")

box.sql.execute("DROP TABLE t;")
box.schema.user.revoke('guest', 'read,write,execute', 'universe')

-- gh-3857 "PRAGMA collation_list" invokes segmentation fault.
box.schema.user.create('tmp')
box.session.su('tmp')
-- Error: read access to space is denied.
box.sql.execute("pragma collation_list")
box.session.su('admin')
box.schema.user.drop('tmp')

-- gh-3644 Foreign key update fails with "unicode_ci".
-- Check that foreign key update doesn't fail with "unicode_ci".
box.sql.execute('CREATE TABLE t0 (s1 CHAR(5) COLLATE "unicode_ci" UNIQUE, id INT PRIMARY KEY AUTOINCREMENT);')
box.sql.execute('CREATE TABLE t1 (s1 INT PRIMARY KEY, s0 CHAR(5) COLLATE "unicode_ci" REFERENCES t0(s1));')
box.sql.execute("INSERT INTO t0(s1) VALUES ('a');")
box.sql.execute("INSERT INTO t1 VALUES (1,'a');")
-- Should't fail.
box.sql.execute("UPDATE t0 SET s1 = 'A';")
box.sql.execute("SELECT s1 FROM t0;")
box.sql.execute("SELECT * FROM t1;")
box.sql.execute("DROP TABLE t1;")
box.sql.execute("DROP TABLE t0;")
-- Check that foreign key update fails with default collation.
box.sql.execute('CREATE TABLE t0 (s1 CHAR(5) UNIQUE, id INT PRIMARY KEY AUTOINCREMENT);')
box.sql.execute('CREATE TABLE t1 (s1 INT PRIMARY KEY, s0 CHAR(5) REFERENCES t0(s1));')
box.sql.execute("INSERT INTO t0(s1) VALUES ('a');")
box.sql.execute("INSERT INTO t1 VALUES (1,'a');")
-- Should fail.
box.sql.execute("UPDATE t0 SET s1 = 'A';")
box.sql.execute("SELECT * FROM t1;")
box.sql.execute("SELECT s1 FROM t0;")
box.sql.execute("DROP TABLE t1;")
box.sql.execute("DROP TABLE t0;")

-- gh-3937: result of concatination has derived collation.
--
box.sql.execute("CREATE TABLE t4a(a TEXT COLLATE \"unicode\", b TEXT COLLATE \"unicode_ci\", c INT PRIMARY KEY);")
box.sql.execute("INSERT INTO t4a VALUES('ABC','abc',1);")
box.sql.execute("INSERT INTO t4a VALUES('ghi','ghi',3);")
-- Only LHS of concatenation has implicitly set collation.
-- Hence, no collation is used.
--
box.sql.execute("SELECT c FROM t4a WHERE (a||'') = b;")
-- BINARY collation is forced for comparison operator as
-- a derivation from concatenation.
--
box.sql.execute("SELECT c FROM t4a WHERE (a COLLATE \"binary\"||'') = b;")
-- Both operands of concatenation have explicit but different
-- collations.
--
box.sql.execute("SELECT c FROM t4a WHERE (a COLLATE \"binary\"||'' COLLATE \"unicode_ci\") = b;")
box.sql.execute("SELECT c FROM t4a WHERE (a COLLATE \"binary\"||'') = b COLLATE \"unicode\";")
-- No collation is used since LHS and RHS of concatenation
-- operator have different implicit collations.
--
box.sql.execute("SELECT c FROM t4a WHERE (a||'')=(b||'');")
box.sql.execute("SELECT c FROM t4a WHERE (a||b)=(b||a);")

box.sql.execute("CREATE TABLE t4b(a TEXT COLLATE \"unicode_ci\", b TEXT COLLATE \"unicode_ci\", c INT PRIMARY KEY);")
box.sql.execute("INSERT INTO t4b VALUES('BCD','bcd',1);")
box.sql.execute("INSERT INTO t4b VALUES('ghi','ghi',3);")
-- Operands have the same implicit collation, so it is derived.
--
box.sql.execute("SELECT c FROM t4a WHERE (a||b)=(b||a);")
-- Couple of other possible combinations.
--
box.sql.execute("SELECT c FROM t4a WHERE (a||b COLLATE \"binary\")=(b||a);")
box.sql.execute("SELECT c FROM t4a WHERE (a||b COLLATE \"binary\")=(b COLLATE \"unicode_ci\"||a);")

box.sql.execute("INSERT INTO t4b VALUES('abc', 'xxx', 2);")
box.sql.execute("INSERT INTO t4b VALUES('gHz', 'xxx', 4);")
-- Results are sorted with case-insensitive order.
-- Otherwise capital latters come first.
--
box.sql.execute("SELECT a FROM t4b ORDER BY a COLLATE \"unicode_ci\" || ''")
box.sql.execute("SELECT a FROM t4b ORDER BY a || b")

box.space.T4A:drop()
box.space.T4B:drop()
