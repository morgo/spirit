// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

type visitor struct{}

func (v visitor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (v visitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type visitor1 struct {
	visitor
}

func (visitor1) Enter(in ast.Node) (ast.Node, bool) {
	return in, true
}

func TestDDLVisitorCoverMisc(t *testing.T) {
	sql := `
create table t (c1 smallint unsigned, c2 int unsigned);
alter table t add column a smallint unsigned after b;
alter table t add column (a int, constraint check (a > 0));
create index t_i on t (id);
create database test character set utf8;
drop database test;
drop index t_i on t;
drop table t;
truncate t;
create table t (
jobAbbr char(4) not null,
constraint foreign key (jobabbr) references ffxi_jobtype (jobabbr) on delete cascade on update cascade
);
`
	parse := parser.New()
	stmts, _, err := parse.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestDMLVistorCover(t *testing.T) {
	sql := `delete from somelog where user = 'jcole' order by timestamp_column limit 1;
delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;
select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1);
insert into t_copy select * from t where t.x > 5;
(select /*+ TIDB_INLJ(t1) */ a from t1 where a=10 and b=1) union (select /*+ TIDB_SMJ(t2) */ a from t2 where a=11 and b=2) order by a limit 10;
update t1 set col1 = col1 + 1, col2 = col1;
show create table t;
load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestSensitiveStatement(t *testing.T) {
	positive := []ast.StmtNode{
		&ast.SetPwdStmt{},
		&ast.CreateUserStmt{},
		&ast.AlterUserStmt{},
		&ast.GrantStmt{},
	}
	for i, stmt := range positive {
		_, ok := stmt.(ast.SensitiveStmtNode)
		require.Truef(t, ok, "%d, %#v fail", i, stmt)
	}

	negative := []ast.StmtNode{
		&ast.DropUserStmt{},
		&ast.RevokeStmt{},
		&ast.AlterTableStmt{},
		&ast.CreateDatabaseStmt{},
		&ast.CreateIndexStmt{},
		&ast.CreateTableStmt{},
		&ast.DropDatabaseStmt{},
		&ast.DropIndexStmt{},
		&ast.DropTableStmt{},
		&ast.RenameTableStmt{},
		&ast.TruncateTableStmt{},
	}
	for _, stmt := range negative {
		_, ok := stmt.(ast.SensitiveStmtNode)
		require.False(t, ok)
	}
}

func TestTableOptimizerHintRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"USE_INDEX(t1 c1)", "USE_INDEX(`t1` `c1`)"},
		{"USE_INDEX(test.t1 c1)", "USE_INDEX(`test`.`t1` `c1`)"},
		{"USE_INDEX(@sel_1 t1 c1)", "USE_INDEX(@`sel_1` `t1` `c1`)"},
		{"USE_INDEX(t1@sel_1 c1)", "USE_INDEX(`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 c1)", "USE_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"USE_INDEX(test.t1@sel_1 partition(p0) c1)", "USE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"FORCE_INDEX(t1 c1)", "FORCE_INDEX(`t1` `c1`)"},
		{"FORCE_INDEX(test.t1 c1)", "FORCE_INDEX(`test`.`t1` `c1`)"},
		{"FORCE_INDEX(@sel_1 t1 c1)", "FORCE_INDEX(@`sel_1` `t1` `c1`)"},
		{"FORCE_INDEX(t1@sel_1 c1)", "FORCE_INDEX(`t1`@`sel_1` `c1`)"},
		{"FORCE_INDEX(test.t1@sel_1 c1)", "FORCE_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"FORCE_INDEX(test.t1@sel_1 partition(p0) c1)", "FORCE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"IGNORE_INDEX(t1 c1)", "IGNORE_INDEX(`t1` `c1`)"},
		{"IGNORE_INDEX(@sel_1 t1 c1)", "IGNORE_INDEX(@`sel_1` `t1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 c1)", "IGNORE_INDEX(`t1`@`sel_1` `c1`)"},
		{"IGNORE_INDEX(t1@sel_1 partition(p0, p1) c1)", "IGNORE_INDEX(`t1`@`sel_1` PARTITION(`p0`, `p1`) `c1`)"},
		{"ORDER_INDEX(t1 c1)", "ORDER_INDEX(`t1` `c1`)"},
		{"ORDER_INDEX(test.t1 c1)", "ORDER_INDEX(`test`.`t1` `c1`)"},
		{"ORDER_INDEX(@sel_1 t1 c1)", "ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"ORDER_INDEX(t1@sel_1 c1)", "ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"ORDER_INDEX(test.t1@sel_1 c1)", "ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"ORDER_INDEX(test.t1@sel_1 partition(p0) c1)", "ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"NO_ORDER_INDEX(t1 c1)", "NO_ORDER_INDEX(`t1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1 c1)", "NO_ORDER_INDEX(`test`.`t1` `c1`)"},
		{"NO_ORDER_INDEX(@sel_1 t1 c1)", "NO_ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"NO_ORDER_INDEX(t1@sel_1 c1)", "NO_ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1@sel_1 c1)", "NO_ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1@sel_1 partition(p0) c1)", "NO_ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(t1 c1)", "INDEX_LOOKUP_PUSHDOWN(`t1` `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(test.t1 c1)", "INDEX_LOOKUP_PUSHDOWN(`test`.`t1` `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(@sel_1 t1 c1)", "INDEX_LOOKUP_PUSHDOWN(@`sel_1` `t1` `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(t1@sel_1 c1)", "INDEX_LOOKUP_PUSHDOWN(`t1`@`sel_1` `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(test.t1@sel_1 c1)", "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` `c1`)"},
		{"INDEX_LOOKUP_PUSHDOWN(test.t1@sel_1 partition(p0) c1)", "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)"},
		{"TIDB_SMJ(`t1`)", "TIDB_SMJ(`t1`)"},
		{"TIDB_SMJ(t1)", "TIDB_SMJ(`t1`)"},
		{"TIDB_SMJ(t1,t2)", "TIDB_SMJ(`t1`, `t2`)"},
		{"TIDB_SMJ(@sel1 t1,t2)", "TIDB_SMJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_SMJ(t1@sel1,t2@sel2)", "TIDB_SMJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"TIDB_INLJ(t1,t2)", "TIDB_INLJ(`t1`, `t2`)"},
		{"TIDB_INLJ(@sel1 t1,t2)", "TIDB_INLJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_INLJ(t1@sel1,t2@sel2)", "TIDB_INLJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"TIDB_HJ(t1,t2)", "TIDB_HJ(`t1`, `t2`)"},
		{"TIDB_HJ(@sel1 t1,t2)", "TIDB_HJ(@`sel1` `t1`, `t2`)"},
		{"TIDB_HJ(t1@sel1,t2@sel2)", "TIDB_HJ(`t1`@`sel1`, `t2`@`sel2`)"},
		{"MERGE_JOIN(t1,t2)", "MERGE_JOIN(`t1`, `t2`)"},
		{"BROADCAST_JOIN(t1,t2)", "BROADCAST_JOIN(`t1`, `t2`)"},
		{"INL_HASH_JOIN(t1,t2)", "INL_HASH_JOIN(`t1`, `t2`)"},
		{"INL_MERGE_JOIN(t1,t2)", "INL_MERGE_JOIN(`t1`, `t2`)"},
		{"INL_JOIN(t1,t2)", "INL_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN(t1,t2)", "HASH_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN_BUILD(t1)", "HASH_JOIN_BUILD(`t1`)"},
		{"HASH_JOIN_PROBE(t1)", "HASH_JOIN_PROBE(`t1`)"},
		{"LEADING(t1)", "LEADING(`t1`)"},
		{"LEADING(t1, c1)", "LEADING(`t1`, `c1`)"},
		{"LEADING((t1, c1), t2)", "LEADING((`t1`, `c1`), `t2`)"},
		{"LEADING(t1, (c1, t2))", "LEADING(`t1`, (`c1`, `t2`))"},
		{"LEADING(((t1, c1), t2), t3)", "LEADING(((`t1`, `c1`), `t2`), `t3`)"},
		{"LEADING(t1, (c1, (t2, t3)))", "LEADING(`t1`, (`c1`, (`t2`, `t3`)))"},
		{"LEADING(t1, c1, t2)", "LEADING(`t1`, `c1`, `t2`)"},
		{"LEADING(@sel1 t1, c1)", "LEADING(@`sel1` `t1`, `c1`)"},
		{"LEADING(@sel1 t1)", "LEADING(@`sel1` `t1`)"},
		{"LEADING(@sel1 t1, c1, t2)", "LEADING(@`sel1` `t1`, `c1`, `t2`)"},
		{"LEADING(@sel1 t1, (c1, t2))", "LEADING(@`sel1` `t1`, (`c1`, `t2`))"},
		{"LEADING(@sel1 t1, (c1, t2), d3)", "LEADING(@`sel1` `t1`, (`c1`, `t2`), `d3`)"},
		{"LEADING(t1@sel1)", "LEADING(`t1`@`sel1`)"},
		{"LEADING(t1@sel1, c1)", "LEADING(`t1`@`sel1`, `c1`)"},
		{"LEADING(t1@sel1, c1, t2)", "LEADING(`t1`@`sel1`, `c1`, `t2`)"},
		{"LEADING((t1@sel1, c1), t2)", "LEADING((`t1`@`sel1`, `c1`), `t2`)"},
		{"LEADING(t1@sel1, (c1, t2))", "LEADING(`t1`@`sel1`, (`c1`, `t2`))"},
		{"LEADING(t1@sel1, c1, t2, d3)", "LEADING(`t1`@`sel1`, `c1`, `t2`, `d3`)"},
		{"LEADING(t1@sel1, (c1, t2), d3)", "LEADING(`t1`@`sel1`, (`c1`, `t2`), `d3`)"},
		{"MAX_EXECUTION_TIME(3000)", "MAX_EXECUTION_TIME(3000)"},
		{"MAX_EXECUTION_TIME(@sel1 3000)", "MAX_EXECUTION_TIME(@`sel1` 3000)"},
		{"USE_INDEX_MERGE(t1 c1)", "USE_INDEX_MERGE(`t1` `c1`)"},
		{"USE_INDEX_MERGE(@sel1 t1 c1)", "USE_INDEX_MERGE(@`sel1` `t1` `c1`)"},
		{"USE_INDEX_MERGE(t1@sel1 c1)", "USE_INDEX_MERGE(`t1`@`sel1` `c1`)"},
		{"USE_TOJA(TRUE)", "USE_TOJA(TRUE)"},
		{"USE_TOJA(FALSE)", "USE_TOJA(FALSE)"},
		{"USE_TOJA(@sel1 TRUE)", "USE_TOJA(@`sel1` TRUE)"},
		{"USE_CASCADES(TRUE)", "USE_CASCADES(TRUE)"},
		{"USE_CASCADES(FALSE)", "USE_CASCADES(FALSE)"},
		{"USE_CASCADES(@sel1 TRUE)", "USE_CASCADES(@`sel1` TRUE)"},
		{"QUERY_TYPE(OLAP)", "QUERY_TYPE(OLAP)"},
		{"QUERY_TYPE(OLTP)", "QUERY_TYPE(OLTP)"},
		{"QUERY_TYPE(@sel1 OLTP)", "QUERY_TYPE(@`sel1` OLTP)"},
		{"NTH_PLAN(10)", "NTH_PLAN(10)"},
		{"NTH_PLAN(@sel1 30)", "NTH_PLAN(@`sel1` 30)"},
		{"MEMORY_QUOTA(1 GB)", "MEMORY_QUOTA(1024 MB)"},
		{"MEMORY_QUOTA(@sel1 1 GB)", "MEMORY_QUOTA(@`sel1` 1024 MB)"},
		{"HASH_AGG()", "HASH_AGG()"},
		{"HASH_AGG(@sel1)", "HASH_AGG(@`sel1`)"},
		{"STREAM_AGG()", "STREAM_AGG()"},
		{"STREAM_AGG(@sel1)", "STREAM_AGG(@`sel1`)"},
		{"AGG_TO_COP()", "AGG_TO_COP()"},
		{"AGG_TO_COP(@sel_1)", "AGG_TO_COP(@`sel_1`)"},
		{"LIMIT_TO_COP()", "LIMIT_TO_COP()"},
		{"MERGE()", "MERGE()"},
		{"STRAIGHT_JOIN()", "STRAIGHT_JOIN()"},
		{"NO_INDEX_MERGE()", "NO_INDEX_MERGE()"},
		{"NO_INDEX_MERGE(@sel1)", "NO_INDEX_MERGE(@`sel1`)"},
		{"READ_CONSISTENT_REPLICA()", "READ_CONSISTENT_REPLICA()"},
		{"READ_CONSISTENT_REPLICA(@sel1)", "READ_CONSISTENT_REPLICA(@`sel1`)"},
		{"QB_NAME(sel1)", "QB_NAME(`sel1`)"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1, t2])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1`, `t2`])"},
		{"READ_FROM_STORAGE(@sel TIFLASH[t1 partition(p0)])", "READ_FROM_STORAGE(@`sel` TIFLASH[`t1` PARTITION(`p0`)])"},
		{"TIME_RANGE('2020-02-02 10:10:10','2020-02-02 11:10:10')", "TIME_RANGE('2020-02-02 10:10:10', '2020-02-02 11:10:10')"},
		{"RESOURCE_GROUP(rg1)", "RESOURCE_GROUP(`rg1`)"},
		{"RESOURCE_GROUP(`default`)", "RESOURCE_GROUP(`default`)"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.SelectStmt).TableHints[0]
	}
	runNodeRestoreTest(t, testCases, "select /*+ %s */ * from t1 join t2", extractNodeFunc)
}
