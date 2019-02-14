/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "box/box.h"
#include "box/session.h"
#include "box/schema.h"
#include "sqlInt.h"
#include "tarantoolInt.h"

struct Table *
sql_lookup_table(struct Parse *parse, struct SrcList_item *tbl_name)
{
	assert(tbl_name != NULL);
	assert(tbl_name->pTab == NULL);
	struct space *space = space_by_name(tbl_name->zName);
	if (space == NULL) {
		diag_set(ClientError, ER_NO_SUCH_SPACE, tbl_name->zName);
		sql_parser_error(parse);
		return NULL;
	}
	assert(space != NULL);
	if (space->def->field_count == 0) {
		diag_set(ClientError, ER_UNSUPPORTED, "SQL",
			 "space without format");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	struct Table *table = sqlDbMallocZero(parse->db, sizeof(*table));
	if (table == NULL)
		return NULL;
	table->def = space->def;
	table->space = space;
	table->nTabRef = 1;
	tbl_name->pTab = table;
	if (sqlIndexedByLookup(parse, tbl_name) != 0)
		table = NULL;
	return table;
}

void
sql_materialize_view(struct Parse *parse, const char *name, struct Expr *where,
		     int cursor)
{
	struct sql *db = parse->db;
	where = sqlExprDup(db, where, 0);
	struct SrcList *from = sqlSrcListAppend(db, NULL, NULL);
	if (from != NULL) {
		assert(from->nSrc == 1);
		from->a[0].zName = sqlDbStrDup(db, name);
		assert(from->a[0].pOn == NULL);
		assert(from->a[0].pUsing == NULL);
	}
	struct Select *select = sqlSelectNew(parse, NULL, from, where, NULL,
						 NULL, NULL, 0, NULL, NULL);
	struct SelectDest dest;
	sqlSelectDestInit(&dest, SRT_EphemTab, cursor, ++parse->nMem);
	sqlSelect(parse, select, &dest);
	sql_select_delete(db, select);
}

void
sql_table_truncate(struct Parse *parse, struct SrcList *tab_list)
{
	assert(tab_list->nSrc == 1);

	struct Vdbe *v = sqlGetVdbe(parse);
	if (v == NULL)
		goto cleanup;

	const char *tab_name = tab_list->a->zName;
	struct space *space = space_by_name(tab_name);
	if (space == NULL) {
		diag_set(ClientError, ER_NO_SUCH_SPACE, tab_name);
		goto tarantool_error;
	}
	if (! rlist_empty(&space->parent_fkey)) {
		const char *err_msg =
			tt_sprintf("can not truncate space '%s' because other "
				   "objects depend on it", space->def->name);
		diag_set(ClientError, ER_SQL, err_msg);
		goto tarantool_error;
	}
	if (space->def->opts.is_view) {
		const char *err_msg =
			tt_sprintf("can not truncate space '%s' because it is "
				   "a view", space->def->name);
		diag_set(ClientError, ER_SQL, err_msg);
		goto tarantool_error;
	}
	sqlVdbeAddOp2(v, OP_Clear, space->def->id, true);
cleanup:
	sqlSrcListDelete(parse->db, tab_list);
	return;

tarantool_error:
	parse->rc = SQL_TARANTOOL_ERROR;
	parse->nErr++;
	goto cleanup;
}

void
sql_table_delete_from(struct Parse *parse, struct SrcList *tab_list,
		      struct Expr *where)
{
	struct sql *db = parse->db;
	if (parse->nErr || db->mallocFailed)
		goto delete_from_cleanup;

	assert(tab_list->nSrc == 1);

	/* Locate the table which we want to delete.  This table
	 * has to be put in an SrcList structure because some of
	 * the subroutines we will be calling are designed to work
	 * with multiple tables and expect an SrcList* parameter
	 * instead of just a Table* parameter.
	 */
	/* Figure out if we have any triggers and if the table
	 * being deleted from is a view.
	 */
	struct sql_trigger *trigger_list = NULL;
	/* True if there are triggers or FKs or subqueries in the
	 * WHERE clause.
	 */
	struct Table *table = sql_lookup_table(parse, tab_list->a);
	if (table == NULL)
		goto delete_from_cleanup;
	assert(table->space != NULL);
	trigger_list = sql_triggers_exist(table, TK_DELETE, NULL, NULL);
	bool is_complex = trigger_list != NULL ||
			  fkey_is_required(table->def->id, NULL);
	struct space *space = table->space;
	bool is_view = space->def->opts.is_view;

	/* If table is really a view, make sure it has been
	 * initialized.
	 */
	if (is_view) {
		if (sql_view_assign_cursors(parse, table->def->opts.sql) != 0)
			goto delete_from_cleanup;

		if (trigger_list == NULL) {
			sqlErrorMsg(parse, "cannot modify %s because it is a"
					" view", space->def->name);
			goto delete_from_cleanup;
		}
	}

	/* Assign cursor numbers to the table and all its indices.
	 */
	int tab_cursor = tab_list->a[0].iCursor = parse->nTab++;
	parse->nTab += space->index_count;

	/* Begin generating code.  */
	struct Vdbe *v = sqlGetVdbe(parse);
	if (v == NULL)
		goto delete_from_cleanup;

	sqlVdbeCountChanges(v);
	sql_set_multi_write(parse, true);

	/* If we are trying to delete from a view, realize that
	 * view into an ephemeral table.
	 */
	if (is_view) {
		sql_materialize_view(parse, space->def->name, where,
				     tab_cursor);
	}

	/* Initialize the counter of the number of rows deleted,
	 * if we are counting rows.
	 */
	int reg_count = -1;
	struct session *user_session = current_session();
	if (user_session->sql_flags & SQL_CountRows) {
		reg_count = ++parse->nMem;
		sqlVdbeAddOp2(v, OP_Integer, 0, reg_count);
	}
	/* Special case: A DELETE without a WHERE clause deletes
	 * everything. It is easier just to erase the whole table.
	 */
	if (where == NULL && !is_complex) {
		assert(!is_view);

		sqlVdbeAddOp1(v, OP_Clear, space->def->id);
		sqlVdbeChangeP5(v, OPFLAG_NCHANGE);

		/* Do not start Tarantool's transaction in case of
		 * truncate optimization. This is workaround until
		 * system tables cannot be changes inside a
		 * transaction (_truncate).
		 */
		parse->initiateTTrans = false;
	} else {
		/* Resolve the column names in the WHERE clause. */
		struct NameContext nc;
		memset(&nc, 0, sizeof(nc));
		nc.pParse = parse;
		nc.pSrcList = tab_list;
		if (sqlResolveExprNames(&nc, where))
			goto delete_from_cleanup;
		uint16_t wcf = WHERE_ONEPASS_DESIRED | WHERE_DUPLICATES_OK |
			WHERE_SEEK_TABLE;
		if (nc.ncFlags & NC_VarSelect)
			is_complex = true;
		wcf |= (is_complex ? 0 : WHERE_ONEPASS_MULTIROW);
		/* Create an ephemeral table used to hold all
		 * primary keys for rows to be deleted. Since VIEW
		 * is held in ephemeral table, there is no PK for
		 * it, so columns should be loaded manually.
		 */
		struct sql_key_info *pk_info = NULL;
		int reg_eph = ++parse->nMem;
		int reg_pk = parse->nMem + 1;
		int pk_len;
		int eph_cursor = parse->nTab++;
		int addr_eph_open = sqlVdbeCurrentAddr(v);
		if (is_view) {
			pk_len = table->def->field_count;
			parse->nMem += pk_len;
			sqlVdbeAddOp2(v, OP_OpenTEphemeral, reg_eph,
					  pk_len);
		} else {
                        assert(space->index_count > 0);
                        pk_info = sql_key_info_new_from_key_def(db,
					space->index[0]->def->key_def);
                        if (pk_info == NULL)
                                goto delete_from_cleanup;
                        pk_len = pk_info->part_count;
                        parse->nMem += pk_len;
			sqlVdbeAddOp4(v, OP_OpenTEphemeral, reg_eph,
					  pk_len, 0,
					  (char *)pk_info, P4_KEYINFO);
		}

		/* Construct a query to find the primary key for
		 * every row to be deleted, based on the WHERE
		 * clause. Set variable one_pass to indicate the
		 * strategy used to implement this delete:
		 *
		 * ONEPASS_OFF:    Two-pass approach - use a FIFO
		 * for PK values.
		 * ONEPASS_SINGLE: One-pass approach - at most one
		 * row deleted.
		 * ONEPASS_MULTI:  One-pass approach - any number
		 * of rows may be deleted.
		 */
		struct WhereInfo *winfo =
		    sqlWhereBegin(parse, tab_list, where, NULL, NULL, wcf,
				      tab_cursor + 1);
		if (winfo == NULL)
			goto delete_from_cleanup;

		/* The write cursors opened by WHERE_ONEPASS */
		int one_pass_cur[2];
		int one_pass = sqlWhereOkOnePass(winfo, one_pass_cur);
		assert(one_pass != ONEPASS_MULTI);
		/* Tarantool: see comment in
		 * sqlWhereOkOnePass.
		 */
		/* assert(is_complex || one_pass != ONEPASS_OFF); */

		/* Keep track of the number of rows to be
		 * deleted.
		 */
		if (user_session->sql_flags & SQL_CountRows)
			sqlVdbeAddOp2(v, OP_AddImm, reg_count, 1);

		/* Extract the primary key for the current row */
		if (!is_view) {
			struct key_part_def *part = pk_info->parts;
			for (int i = 0; i < pk_len; i++, part++) {
				struct space_def *def = space->def;
				sqlExprCodeGetColumnOfTable(v, def,
								tab_cursor,
								part->fieldno,
								reg_pk + i);
			}
		} else {
			for (int i = 0; i < pk_len; i++) {
				sqlVdbeAddOp3(v, OP_Column, tab_cursor,
						  i, reg_pk + i);
			}
		}

		int reg_key;
		int key_len;
		if (one_pass != ONEPASS_OFF) {
			/* For ONEPASS, no need to store the
			 * primary-key. There is only one, so just
			 * keep it in its register(s) and fall
			 * through to the delete code.
			 */
			reg_key = reg_pk;
			/* OP_Found will use an unpacked key */
			key_len = pk_len;
			sqlVdbeChangeToNoop(v, addr_eph_open);
		} else {
			/* Add the PK key for this row to the
			 * temporary table.
			 */
			reg_key = ++parse->nMem;
			/* Zero tells OP_Found to use a composite
			 * key.
			 */
			key_len = 0;
			struct index *pk = space_index(space, 0);
			enum field_type *types = is_view ? NULL :
						 sql_index_type_str(parse->db,
								    pk->def);
			sqlVdbeAddOp4(v, OP_MakeRecord, reg_pk, pk_len,
					  reg_key, (char *)types, P4_DYNAMIC);
			/* Set flag to save memory allocating one
			 * by malloc.
			 */
			sqlVdbeChangeP5(v, 1);
			sqlVdbeAddOp2(v, OP_IdxInsert, reg_key, reg_eph);
		}

		/* If this DELETE cannot use the ONEPASS strategy,
		 * this is the end of the WHERE loop.
		 */
		int addr_bypass = 0;
		if (one_pass != ONEPASS_OFF)
			addr_bypass = sqlVdbeMakeLabel(v);
		else
			sqlWhereEnd(winfo);

		/* Unless this is a view, open cursors for the
		 * table we are deleting from and all its indices.
		 * If this is a view, then the only effect this
		 * statement has is to fire the INSTEAD OF
		 * triggers.
		 */
		if (!is_view) {
			int iAddrOnce = 0;
			if (one_pass == ONEPASS_MULTI) {
				iAddrOnce = sqlVdbeAddOp0(v, OP_Once);
				VdbeCoverage(v);
			}
			sqlVdbeAddOp4(v, OP_IteratorOpen, tab_cursor, 0, 0,
					  (void *) space, P4_SPACEPTR);
			VdbeComment((v, "%s", space->index[0]->def->name));

			if (one_pass == ONEPASS_MULTI)
				sqlVdbeJumpHere(v, iAddrOnce);
		}

		/* Set up a loop over the primary-keys that were
		 * found in the where-clause loop above.
		 */
		int addr_loop = 0;
		if (one_pass != ONEPASS_OFF) {
			/* OP_Found will use an unpacked key. */
			assert(key_len == pk_len);
			assert(pk_info != NULL || table->def->opts.is_view);
			sqlVdbeAddOp4Int(v, OP_NotFound, tab_cursor,
					     addr_bypass, reg_key, key_len);

			VdbeCoverage(v);
		} else {
			sqlVdbeAddOp3(v, OP_IteratorOpen,
					  eph_cursor, 0, reg_eph);
			addr_loop = sqlVdbeAddOp1(v, OP_Rewind, eph_cursor);
			VdbeCoverage(v);
			sqlVdbeAddOp2(v, OP_RowData, eph_cursor, reg_key);
		}

		/* Delete the row */
		int idx_noseek = -1;
		if (!is_complex && one_pass_cur[1] != tab_cursor
		    /* Tarantool: as far as ONEPASS is disabled,
		     * there's no index w/o need of seeking.
		     */
		    && one_pass != ONEPASS_OFF)
			idx_noseek = one_pass_cur[1];

		sql_generate_row_delete(parse, table, trigger_list, tab_cursor,
					reg_key, key_len, true,
					ON_CONFLICT_ACTION_DEFAULT, one_pass,
					idx_noseek);

		/* End of the loop over all primary-keys. */
		if (one_pass != ONEPASS_OFF) {
			sqlVdbeResolveLabel(v, addr_bypass);
			sqlWhereEnd(winfo);
		} else {
			sqlVdbeAddOp2(v, OP_Next, eph_cursor,
					  addr_loop + 1);
			VdbeCoverage(v);
			sqlVdbeJumpHere(v, addr_loop);
		}
	}

	/* Return the number of rows that were deleted. */
	if ((user_session->sql_flags & SQL_CountRows) != 0 &&
	    parse->pTriggerTab != NULL) {
		sqlVdbeAddOp2(v, OP_ResultRow, reg_count, 1);
		sqlVdbeSetNumCols(v, 1);
		sqlVdbeSetColName(v, 0, COLNAME_NAME, "rows deleted",
				      SQL_STATIC);
	}

 delete_from_cleanup:
	sqlSrcListDelete(db, tab_list);
	sql_expr_delete(db, where, false);
}

void
sql_generate_row_delete(struct Parse *parse, struct Table *table,
			struct sql_trigger *trigger_list, int cursor,
			int reg_pk, short npk, bool need_update_count,
			enum on_conflict_action onconf, u8 mode,
			int idx_noseek)
{
	struct Vdbe *v = parse->pVdbe;
	/* Vdbe is guaranteed to have been allocated by this
	 * stage.
	 */
	assert(v != NULL);
	VdbeModuleComment((v, "BEGIN: GenRowDel(%d,%d,%d,%d)",
			   cursor, iIdxCur, reg_pk, (int)nPk));

	/* Seek cursor iCur to the row to delete. If this row no
	 * longer exists (this can happen if a trigger program has
	 * already deleted it), do not attempt to delete it or
	 * fire any DELETE triggers.
	 */
	int label = sqlVdbeMakeLabel(v);
	if (mode == ONEPASS_OFF) {
		sqlVdbeAddOp4Int(v, OP_NotFound, cursor, label, reg_pk, npk);
		VdbeCoverageIf(v, opSeek == OP_NotFound);
	}

	int first_old_reg = 0;
	/* If there are any triggers to fire, allocate a range of registers to
	 * use for the old.* references in the triggers.
	 */
	if (table != NULL &&
	    (fkey_is_required(table->def->id, NULL) || trigger_list != NULL)) {
		/* Mask of OLD.* columns in use */
		/* TODO: Could use temporary registers here. */
		uint32_t mask =
			sql_trigger_colmask(parse, trigger_list, 0, 0,
					    TRIGGER_BEFORE | TRIGGER_AFTER,
					    table, onconf);
		struct space *space = space_by_id(table->def->id);
		assert(space != NULL);
		mask |= space->fkey_mask;
		first_old_reg = parse->nMem + 1;
		parse->nMem += (1 + (int)table->def->field_count);

		/* Populate the OLD.* pseudo-table register array.
		 * These values will be used by any BEFORE and
		 * AFTER triggers that exist.
		 */
		sqlVdbeAddOp2(v, OP_Copy, reg_pk, first_old_reg);
		for (int i = 0; i < (int)table->def->field_count; i++) {
			testcase(mask != 0xffffffff && iCol == 31);
			testcase(mask != 0xffffffff && iCol == 32);
			if (mask == 0xffffffff
			    || (i <= 31 && (mask & MASKBIT32(i)) != 0)) {
				sqlExprCodeGetColumnOfTable(v, table->def,
								cursor, i,
								first_old_reg +
								i + 1);
			}
		}

		/* Invoke BEFORE DELETE trigger programs. */
		int addr_start = sqlVdbeCurrentAddr(v);
		vdbe_code_row_trigger(parse, trigger_list, TK_DELETE, NULL,
				      TRIGGER_BEFORE, table, first_old_reg,
				      onconf, label);

		/* If any BEFORE triggers were coded, then seek
		 * the cursor to the row to be deleted again. It
		 * may be that the BEFORE triggers moved the
		 * cursor or of already deleted the row that the
		 * cursor was pointing to.
		 */
		if (addr_start < sqlVdbeCurrentAddr(v)) {
			sqlVdbeAddOp4Int(v, OP_NotFound, cursor, label,
					     reg_pk, npk);
			VdbeCoverageIf(v, opSeek == OP_NotFound);
		}

		/* Do FK processing. This call checks that any FK
		 * constraints that refer to this table (i.e.
		 * constraints attached to other tables) are not
		 * violated by deleting this row.
		 */
		fkey_emit_check(parse, table, first_old_reg, 0, NULL);
	}

	/* Delete the index and table entries. Skip this step if
	 * table is really a view (in which case the only effect
	 * of the DELETE statement is to fire the INSTEAD OF
	 * triggers).
	 */
	if (table == NULL || !table->def->opts.is_view) {
		uint8_t p5 = 0;
		sqlVdbeAddOp2(v, OP_Delete, cursor,
				  (need_update_count ? OPFLAG_NCHANGE : 0));
		if (mode != ONEPASS_OFF)
			sqlVdbeChangeP5(v, OPFLAG_AUXDELETE);

		if (idx_noseek >= 0)
			sqlVdbeAddOp1(v, OP_Delete, idx_noseek);

		if (mode == ONEPASS_MULTI)
			p5 |= OPFLAG_SAVEPOSITION;
		sqlVdbeChangeP5(v, p5);
	}

	if (table != NULL) {
		/* Do any ON CASCADE, SET NULL or SET DEFAULT
		 * operations required to handle rows (possibly
		 * in other tables) that refer via a foreign
		 * key to the row just deleted.
		 */

		fkey_emit_actions(parse, table, first_old_reg, NULL);

		/* Invoke AFTER DELETE trigger programs. */
		vdbe_code_row_trigger(parse, trigger_list, TK_DELETE, 0,
				      TRIGGER_AFTER, table, first_old_reg,
				      onconf, label);
	}

	/* Jump here if the row had already been deleted before
	 * any BEFORE trigger programs were invoked. Or if a trigger program
	 * throws a RAISE(IGNORE) exception.
	 */
	sqlVdbeResolveLabel(v, label);
	VdbeModuleComment((v, "END: GenRowDel()"));
}

int
sql_generate_index_key(struct Parse *parse, struct index *index, int cursor,
		       int reg_out, struct index *prev, int reg_prev)
{
	struct Vdbe *v = parse->pVdbe;
	int col_cnt = index->def->key_def->part_count;
	int reg_base = sqlGetTempRange(parse, col_cnt);
	if (prev != NULL && reg_base != reg_prev)
		prev = NULL;
	struct space *space = space_by_id(index->def->space_id);
	assert(space != NULL);
	for (int j = 0; j < col_cnt; j++) {
		if (prev != NULL && prev->def->key_def->parts[j].fieldno ==
				    index->def->key_def->parts[j].fieldno) {
			/*
			 * This column was already computed by the
			 * previous index.
			 */
			continue;
		}
		uint32_t tabl_col = index->def->key_def->parts[j].fieldno;
		sqlExprCodeGetColumnOfTable(v, space->def, cursor, tabl_col,
						reg_base + j);
		/*
		 * If the column type is NUMBER but the number
		 * is an integer, then it might be stored in the
		 * table as an integer (using a compact
		 * representation) then converted to REAL by an
		 * OP_Realify opcode. But we are getting
		 * ready to store this value back into an index,
		 * where it should be converted by to INTEGER
		 * again.  So omit the OP_Realify opcode if
		 * it is present
		 */
		sqlVdbeDeletePriorOpcode(v, OP_Realify);
	}
	if (reg_out != 0)
		sqlVdbeAddOp3(v, OP_MakeRecord, reg_base, col_cnt, reg_out);

	sqlReleaseTempRange(parse, reg_base, col_cnt);
	return reg_base;
}
