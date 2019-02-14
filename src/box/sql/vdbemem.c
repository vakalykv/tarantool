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

/*
 *
 * This file contains code use to manipulate "Mem" structure.  A "Mem"
 * stores a single value in the VDBE.  Mem is an opaque structure visible
 * only within the VDBE.  Interface routines refer to a Mem using the
 * name sql_value
 */
#include "sqlInt.h"
#include "vdbeInt.h"
#include "tarantoolInt.h"
#include "box/schema.h"
#include "box/tuple.h"
#include "mpstream.h"

#ifdef SQL_DEBUG
/*
 * Check invariants on a Mem object.
 *
 * This routine is intended for use inside of assert() statements, like
 * this:    assert( sqlVdbeCheckMemInvariants(pMem) );
 */
int
sqlVdbeCheckMemInvariants(Mem * p)
{
	/* If MEM_Dyn is set then Mem.xDel!=0.
	 * Mem.xDel is might not be initialized if MEM_Dyn is clear.
	 */
	assert((p->flags & MEM_Dyn) == 0 || p->xDel != 0);

	/* MEM_Dyn may only be set if Mem.szMalloc==0.  In this way we
	 * ensure that if Mem.szMalloc>0 then it is safe to do
	 * Mem.z = Mem.zMalloc without having to check Mem.flags&MEM_Dyn.
	 * That saves a few cycles in inner loops.
	 */
	assert((p->flags & MEM_Dyn) == 0 || p->szMalloc == 0);

	/* Cannot be both MEM_Int and MEM_Real at the same time */
	assert((p->flags & (MEM_Int | MEM_Real)) != (MEM_Int | MEM_Real));

	/* The szMalloc field holds the correct memory allocation size */
	assert(p->szMalloc == 0
	       || p->szMalloc == sqlDbMallocSize(p->db, p->zMalloc));

	/* If p holds a string or blob, the Mem.z must point to exactly
	 * one of the following:
	 *
	 *   (1) Memory in Mem.zMalloc and managed by the Mem object
	 *   (2) Memory to be freed using Mem.xDel
	 *   (3) An ephemeral string or blob
	 *   (4) A static string or blob
	 */
	if ((p->flags & (MEM_Str | MEM_Blob)) && p->n > 0) {
		assert(((p->szMalloc > 0 && p->z == p->zMalloc) ? 1 : 0) +
		       ((p->flags & MEM_Dyn) != 0 ? 1 : 0) +
		       ((p->flags & MEM_Ephem) != 0 ? 1 : 0) +
		       ((p->flags & MEM_Static) != 0 ? 1 : 0) == 1);
	}
	return 1;
}
#endif

/*
 * Make sure pMem->z points to a writable allocation of at least
 * min(n,32) bytes.
 *
 * If the bPreserve argument is true, then copy of the content of
 * pMem->z into the new allocation.  pMem must be either a string or
 * blob if bPreserve is true.  If bPreserve is false, any prior content
 * in pMem->z is discarded.
 */
SQL_NOINLINE int
sqlVdbeMemGrow(Mem * pMem, int n, int bPreserve)
{
	assert(sqlVdbeCheckMemInvariants(pMem));
	testcase(pMem->db == 0);

	/* If the bPreserve flag is set to true, then the memory cell must already
	 * contain a valid string or blob value.
	 */
	assert(bPreserve == 0 || pMem->flags & (MEM_Blob | MEM_Str));
	testcase(bPreserve && pMem->z == 0);

	assert(pMem->szMalloc == 0
	       || pMem->szMalloc == sqlDbMallocSize(pMem->db,
							pMem->zMalloc));
	if (pMem->szMalloc < n) {
		if (n < 32)
			n = 32;
		if (bPreserve && pMem->szMalloc > 0 && pMem->z == pMem->zMalloc) {
			pMem->z = pMem->zMalloc =
			    sqlDbReallocOrFree(pMem->db, pMem->z, n);
			bPreserve = 0;
		} else {
			if (pMem->szMalloc > 0)
				sqlDbFree(pMem->db, pMem->zMalloc);
			pMem->zMalloc = sqlDbMallocRaw(pMem->db, n);
		}
		if (pMem->zMalloc == 0) {
			sqlVdbeMemSetNull(pMem);
			pMem->z = 0;
			pMem->szMalloc = 0;
			return SQL_NOMEM_BKPT;
		} else {
			pMem->szMalloc =
			    sqlDbMallocSize(pMem->db, pMem->zMalloc);
		}
	}

	if (bPreserve && pMem->z && pMem->z != pMem->zMalloc) {
		memcpy(pMem->zMalloc, pMem->z, pMem->n);
	}
	if ((pMem->flags & MEM_Dyn) != 0) {
		assert(pMem->xDel != 0 && pMem->xDel != SQL_DYNAMIC);
		pMem->xDel((void *)(pMem->z));
	}

	pMem->z = pMem->zMalloc;
	pMem->flags &= ~(MEM_Dyn | MEM_Ephem | MEM_Static);
	return SQL_OK;
}

/*
 * Change the pMem->zMalloc allocation to be at least szNew bytes.
 * If pMem->zMalloc already meets or exceeds the requested size, this
 * routine is a no-op.
 *
 * Any prior string or blob content in the pMem object may be discarded.
 * The pMem->xDel destructor is called, if it exists.  Though MEM_Str
 * and MEM_Blob values may be discarded, MEM_Int, MEM_Real, and MEM_Null
 * values are preserved.
 *
 * Return SQL_OK on success or an error code (probably SQL_NOMEM)
 * if unable to complete the resizing.
 */
int
sqlVdbeMemClearAndResize(Mem * pMem, int szNew)
{
	assert(szNew > 0);
	assert((pMem->flags & MEM_Dyn) == 0 || pMem->szMalloc == 0);
	if (pMem->szMalloc < szNew) {
		return sqlVdbeMemGrow(pMem, szNew, 0);
	}
	assert((pMem->flags & MEM_Dyn) == 0);
	pMem->z = pMem->zMalloc;
	pMem->flags &= (MEM_Null | MEM_Int | MEM_Real);
	return SQL_OK;
}

/*
 * Change pMem so that its MEM_Str or MEM_Blob value is stored in
 * MEM.zMalloc, where it can be safely written.
 *
 * Return SQL_OK on success or SQL_NOMEM if malloc fails.
 */
int
sqlVdbeMemMakeWriteable(Mem * pMem)
{
	if ((pMem->flags & (MEM_Str | MEM_Blob)) != 0) {
		if (ExpandBlob(pMem))
			return SQL_NOMEM;
		if (pMem->szMalloc == 0 || pMem->z != pMem->zMalloc) {
			if (sqlVdbeMemGrow(pMem, pMem->n + 2, 1)) {
				return SQL_NOMEM_BKPT;
			}
			pMem->z[pMem->n] = 0;
			pMem->z[pMem->n + 1] = 0;
			pMem->flags |= MEM_Term;
		}
	}
	pMem->flags &= ~MEM_Ephem;
#ifdef SQL_DEBUG
	pMem->pScopyFrom = 0;
#endif

	return SQL_OK;
}

/*
 * If the given Mem* has a zero-filled tail, turn it into an ordinary
 * blob stored in dynamically allocated space.
 */
#ifndef SQL_OMIT_INCRBLOB
int
sqlVdbeMemExpandBlob(Mem * pMem)
{
	int nByte;
	assert(pMem->flags & MEM_Zero);
	assert(pMem->flags & MEM_Blob);

	/* Set nByte to the number of bytes required to store the expanded blob. */
	nByte = pMem->n + pMem->u.nZero;
	if (nByte <= 0) {
		nByte = 1;
	}
	if (sqlVdbeMemGrow(pMem, nByte, 1)) {
		return SQL_NOMEM_BKPT;
	}

	memset(&pMem->z[pMem->n], 0, pMem->u.nZero);
	pMem->n += pMem->u.nZero;
	pMem->flags &= ~(MEM_Zero | MEM_Term);
	return SQL_OK;
}
#endif

/*
 * It is already known that pMem contains an unterminated string.
 * Add the zero terminator.
 */
static SQL_NOINLINE int
vdbeMemAddTerminator(Mem * pMem)
{
	if (sqlVdbeMemGrow(pMem, pMem->n + 2, 1)) {
		return SQL_NOMEM_BKPT;
	}
	pMem->z[pMem->n] = 0;
	pMem->z[pMem->n + 1] = 0;
	pMem->flags |= MEM_Term;
	return SQL_OK;
}

/*
 * Make sure the given Mem is \u0000 terminated.
 */
int
sqlVdbeMemNulTerminate(Mem * pMem)
{
	testcase((pMem->flags & (MEM_Term | MEM_Str)) == (MEM_Term | MEM_Str));
	testcase((pMem->flags & (MEM_Term | MEM_Str)) == 0);
	if ((pMem->flags & (MEM_Term | MEM_Str)) != MEM_Str) {
		return SQL_OK;	/* Nothing to do */
	} else {
		return vdbeMemAddTerminator(pMem);
	}
}

/*
 * Add MEM_Str to the set of representations for the given Mem.  Numbers
 * are converted using sql_snprintf().  Converting a BLOB to a string
 * is a no-op.
 *
 * Existing representations MEM_Int and MEM_Real are invalidated if
 * bForce is true but are retained if bForce is false.
 *
 * A MEM_Null value will never be passed to this function. This function is
 * used for converting values to text for returning to the user (i.e. via
 * sql_value_text()), or for ensuring that values to be used as btree
 * keys are strings. In the former case a NULL pointer is returned the
 * user and the latter is an internal programming error.
 */
int
sqlVdbeMemStringify(Mem * pMem, u8 bForce)
{
	int fg = pMem->flags;
	const int nByte = 32;

	if ((fg & (MEM_Null | MEM_Str | MEM_Blob)) != 0)
		return SQL_OK;

	assert(!(fg & MEM_Zero));
	assert(fg & (MEM_Int | MEM_Real));
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	if (sqlVdbeMemClearAndResize(pMem, nByte)) {
		return SQL_NOMEM_BKPT;
	}
	if (fg & MEM_Int) {
		sql_snprintf(nByte, pMem->z, "%lld", pMem->u.i);
	} else {
		assert(fg & MEM_Real);
		sql_snprintf(nByte, pMem->z, "%!.15g", pMem->u.r);
	}
	pMem->n = sqlStrlen30(pMem->z);
	pMem->flags |= MEM_Str | MEM_Term;
	if (bForce)
		pMem->flags &= ~(MEM_Int | MEM_Real);
	return SQL_OK;
}

/*
 * Memory cell pMem contains the context of an aggregate function.
 * This routine calls the finalize method for that function.  The
 * result of the aggregate is stored back into pMem.
 *
 * Return SQL_ERROR if the finalizer reports an error.  SQL_OK
 * otherwise.
 */
int
sqlVdbeMemFinalize(Mem * pMem, FuncDef * pFunc)
{
	int rc = SQL_OK;
	if (ALWAYS(pFunc && pFunc->xFinalize)) {
		sql_context ctx;
		Mem t;
		assert((pMem->flags & MEM_Null) != 0 || pFunc == pMem->u.pDef);
		memset(&ctx, 0, sizeof(ctx));
		memset(&t, 0, sizeof(t));
		t.flags = MEM_Null;
		t.db = pMem->db;
		ctx.pOut = &t;
		ctx.pMem = pMem;
		ctx.pFunc = pFunc;
		pFunc->xFinalize(&ctx);	/* IMP: R-24505-23230 */
		assert((pMem->flags & MEM_Dyn) == 0);
		if (pMem->szMalloc > 0)
			sqlDbFree(pMem->db, pMem->zMalloc);
		memcpy(pMem, &t, sizeof(t));
		rc = ctx.isError;
	}
	return rc;
}

/*
 * If the memory cell contains a value that must be freed by
 * invoking the external callback in Mem.xDel, then this routine
 * will free that value.  It also sets Mem.flags to MEM_Null.
 *
 * This is a helper routine for sqlVdbeMemSetNull() and
 * for sqlVdbeMemRelease().  Use those other routines as the
 * entry point for releasing Mem resources.
 */
static SQL_NOINLINE void
vdbeMemClearExternAndSetNull(Mem * p)
{
	assert(VdbeMemDynamic(p));
	if (p->flags & MEM_Agg) {
		sqlVdbeMemFinalize(p, p->u.pDef);
		assert((p->flags & MEM_Agg) == 0);
		testcase(p->flags & MEM_Dyn);
	}
	if (p->flags & MEM_Dyn) {
		assert(p->xDel != SQL_DYNAMIC && p->xDel != 0);
		p->xDel((void *)p->z);
	} else if (p->flags & MEM_Frame) {
		VdbeFrame *pFrame = p->u.pFrame;
		pFrame->pParent = pFrame->v->pDelFrame;
		pFrame->v->pDelFrame = pFrame;
	}
	p->flags = MEM_Null;
}

/*
 * Release memory held by the Mem p, both external memory cleared
 * by p->xDel and memory in p->zMalloc.
 *
 * This is a helper routine invoked by sqlVdbeMemRelease() in
 * the unusual case where there really is memory in p that needs
 * to be freed.
 */
static SQL_NOINLINE void
vdbeMemClear(Mem * p)
{
	if (VdbeMemDynamic(p)) {
		vdbeMemClearExternAndSetNull(p);
	}
	if (p->szMalloc) {
		sqlDbFree(p->db, p->zMalloc);
		p->szMalloc = 0;
	}
	p->z = 0;
}

/*
 * Release any memory resources held by the Mem.  Both the memory that is
 * free by Mem.xDel and the Mem.zMalloc allocation are freed.
 *
 * Use this routine prior to clean up prior to abandoning a Mem, or to
 * reset a Mem back to its minimum memory utilization.
 *
 * Use sqlVdbeMemSetNull() to release just the Mem.xDel space
 * prior to inserting new content into the Mem.
 */
void
sqlVdbeMemRelease(Mem * p)
{
	assert(sqlVdbeCheckMemInvariants(p));
	if (VdbeMemDynamic(p) || p->szMalloc) {
		vdbeMemClear(p);
	}
}

/*
 * Convert a 64-bit IEEE double into a 64-bit signed integer.
 * If the double is out of range of a 64-bit signed integer then
 * return the closest available 64-bit signed integer.
 */
static int
doubleToInt64(double r, int64_t *i)
{
#ifdef SQL_OMIT_FLOATING_POINT
	/* When floating-point is omitted, double and int64 are the same thing */
	*i = r;
	return 0;
#else
	/*
	 * Many compilers we encounter do not define constants for the
	 * minimum and maximum 64-bit integers, or they define them
	 * inconsistently.  And many do not understand the "LL" notation.
	 * So we define our own static constants here using nothing
	 * larger than a 32-bit integer constant.
	 */
	static const int64_t maxInt = LARGEST_INT64;
	static const int64_t minInt = SMALLEST_INT64;

	if (r <= (double)minInt) {
		*i = minInt;
		return -1;
	} else if (r >= (double)maxInt) {
		*i = maxInt;
		return -1;
	} else {
		*i = (int64_t) r;
		return *i != r;
	}
#endif
}

/*
 * Return some kind of integer value which is the best we can do
 * at representing the value that *pMem describes as an integer.
 * If pMem is an integer, then the value is exact.  If pMem is
 * a floating-point then the value returned is the integer part.
 * If pMem is a string or blob, then we make an attempt to convert
 * it into an integer and return that.  If pMem represents an
 * an SQL-NULL value, return 0.
 *
 * If pMem represents a string value, its encoding might be changed.
 */
int
sqlVdbeIntValue(Mem * pMem, int64_t *i)
{
	int flags;
	assert(EIGHT_BYTE_ALIGNMENT(pMem));
	flags = pMem->flags;
	if (flags & MEM_Int) {
		*i = pMem->u.i;
		return 0;
	} else if (flags & MEM_Real) {
		return doubleToInt64(pMem->u.r, i);
	} else if (flags & (MEM_Str)) {
		assert(pMem->z || pMem->n == 0);
		if (sql_atoi64(pMem->z, (int64_t *)i, pMem->n) == 0)
			return 0;
	}
	return -1;
}

/*
 * Return the best representation of pMem that we can get into a
 * double.  If pMem is already a double or an integer, return its
 * value.  If it is a string or blob, try to convert it to a double.
 * If it is a NULL, return 0.0.
 */
int
sqlVdbeRealValue(Mem * pMem, double *v)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));
	if (pMem->flags & MEM_Real) {
		*v = pMem->u.r;
		return 0;
	} else if (pMem->flags & MEM_Int) {
		*v = (double)pMem->u.i;
		return 0;
	} else if (pMem->flags & MEM_Str) {
		if (sqlAtoF(pMem->z, v, pMem->n))
			return 0;
	}
	return -1;
}

/*
 * The MEM structure is already a MEM_Real.  Try to also make it a
 * MEM_Int if we can.
 */
int
mem_apply_integer_type(Mem *pMem)
{
	int rc;
	i64 ix;
	assert(pMem->flags & MEM_Real);
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	if ((rc = doubleToInt64(pMem->u.r, (int64_t *) &ix)) == 0) {
		pMem->u.i = ix;
		MemSetTypeFlag(pMem, MEM_Int);
	}
	return rc;
}

/*
 * Convert pMem to type integer.  Invalidate any prior representations.
 */
int
sqlVdbeMemIntegerify(Mem * pMem, bool is_forced)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	int64_t i;
	if (sqlVdbeIntValue(pMem, &i) == 0) {
		pMem->u.i = i;
		MemSetTypeFlag(pMem, MEM_Int);
		return 0;
	} else if ((pMem->flags & MEM_Real) != 0 && is_forced) {
		pMem->u.i = (int) pMem->u.r;
		MemSetTypeFlag(pMem, MEM_Int);
		return 0;
	}

	double d;
	if (sqlVdbeRealValue(pMem, &d) || (int64_t) d != d) {
		return SQL_ERROR;
	}
	pMem->u.i = (int64_t) d;
	MemSetTypeFlag(pMem, MEM_Int);
	return 0;
}

/*
 * Convert pMem so that it is of type MEM_Real.
 * Invalidate any prior representations.
 */
int
sqlVdbeMemRealify(Mem * pMem)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));
	double v;
	if (sqlVdbeRealValue(pMem, &v))
		return SQL_ERROR;

	pMem->u.r = v;
	MemSetTypeFlag(pMem, MEM_Real);
	return SQL_OK;
}

/*
 * Convert pMem so that it has types MEM_Real or MEM_Int or both.
 * Invalidate any prior representations.
 *
 * Every effort is made to force the conversion, even if the input
 * is a string that does not look completely like a number.  Convert
 * as much of the string as we can and ignore the rest.
 */
int
sqlVdbeMemNumerify(Mem * pMem)
{
	if ((pMem->flags & (MEM_Int | MEM_Real | MEM_Null)) == 0) {
		assert((pMem->flags & (MEM_Blob | MEM_Str)) != 0);
		if (0 == sql_atoi64(pMem->z, (int64_t *)&pMem->u.i, pMem->n)) {
			MemSetTypeFlag(pMem, MEM_Int);
		} else {
			double v;
			if (sqlVdbeRealValue(pMem, &v))
				return SQL_ERROR;
			pMem->u.r = v;
			MemSetTypeFlag(pMem, MEM_Real);
			mem_apply_integer_type(pMem);
		}
	}
	assert((pMem->flags & (MEM_Int | MEM_Real | MEM_Null)) != 0);
	pMem->flags &= ~(MEM_Str | MEM_Blob | MEM_Zero);
	return SQL_OK;
}

/*
 * Cast the datatype of the value in pMem according to the type
 * @type.  Casting is different from applying type in that a cast
 * is forced.  In other words, the value is converted into the desired
 * type even if that results in loss of data.  This routine is
 * used (for example) to implement the SQL "cast()" operator.
 */
int
sqlVdbeMemCast(Mem * pMem, enum field_type type)
{
	assert(type < field_type_MAX);
	if (pMem->flags & MEM_Null)
		return SQL_OK;
	if ((pMem->flags & MEM_Blob) != 0 && type == FIELD_TYPE_NUMBER) {
		if (sql_atoi64(pMem->z, (int64_t *) &pMem->u.i, pMem->n) == 0) {
			MemSetTypeFlag(pMem, MEM_Real);
			pMem->u.r = pMem->u.i;
			return 0;
		}
		return ! sqlAtoF(pMem->z, &pMem->u.r, pMem->n);
	}
	switch (type) {
	case FIELD_TYPE_SCALAR:
		if (pMem->flags & MEM_Blob)
			return SQL_OK;
		if (pMem->flags & MEM_Str) {
			MemSetTypeFlag(pMem, MEM_Blob);
			return SQL_OK;
		}
		if (pMem->flags & MEM_Int || pMem->flags & MEM_Real) {
			if (sqlVdbeMemStringify(pMem, 1) != 0)
				return -1;
			MemSetTypeFlag(pMem, MEM_Blob);
			return 0;
		}
		return SQL_ERROR;
	case FIELD_TYPE_INTEGER:
		if ((pMem->flags & MEM_Blob) != 0) {
			if (sql_atoi64(pMem->z, (int64_t *) &pMem->u.i,
				       pMem->n) != 0)
				return -1;
			MemSetTypeFlag(pMem, MEM_Int);
			return 0;
		}
		return sqlVdbeMemIntegerify(pMem, true);
	case FIELD_TYPE_NUMBER:
		return sqlVdbeMemRealify(pMem);
	default:
		assert(type == FIELD_TYPE_STRING);
		assert(MEM_Str == (MEM_Blob >> 3));
		pMem->flags |= (pMem->flags & MEM_Blob) >> 3;
			sql_value_apply_type(pMem, FIELD_TYPE_STRING);
		assert(pMem->flags & MEM_Str || pMem->db->mallocFailed);
		pMem->flags &= ~(MEM_Int | MEM_Real | MEM_Blob | MEM_Zero);
		return SQL_OK;
	}
}

/*
 * Initialize bulk memory to be a consistent Mem object.
 *
 * The minimum amount of initialization feasible is performed.
 */
void
sqlVdbeMemInit(Mem * pMem, sql * db, u32 flags)
{
	assert((flags & ~MEM_TypeMask) == 0);
	pMem->flags = flags;
	pMem->db = db;
	pMem->szMalloc = 0;
}

/*
 * Delete any previous value and set the value stored in *pMem to NULL.
 *
 * This routine calls the Mem.xDel destructor to dispose of values that
 * require the destructor.  But it preserves the Mem.zMalloc memory allocation.
 * To free all resources, use sqlVdbeMemRelease(), which both calls this
 * routine to invoke the destructor and deallocates Mem.zMalloc.
 *
 * Use this routine to reset the Mem prior to insert a new value.
 *
 * Use sqlVdbeMemRelease() to complete erase the Mem prior to abandoning it.
 */
void
sqlVdbeMemSetNull(Mem * pMem)
{
	if (VdbeMemDynamic(pMem)) {
		vdbeMemClearExternAndSetNull(pMem);
	} else {
		pMem->flags = MEM_Null;
	}
}

void
sqlValueSetNull(sql_value * p)
{
	sqlVdbeMemSetNull((Mem *) p);
}

/*
 * Delete any previous value and set the value to be a BLOB of length
 * n containing all zeros.
 */
void
sqlVdbeMemSetZeroBlob(Mem * pMem, int n)
{
	sqlVdbeMemRelease(pMem);
	pMem->flags = MEM_Blob | MEM_Zero;
	pMem->n = 0;
	if (n < 0)
		n = 0;
	pMem->u.nZero = n;
	pMem->z = 0;
}

/*
 * The pMem is known to contain content that needs to be destroyed prior
 * to a value change.  So invoke the destructor, then set the value to
 * a 64-bit integer.
 */
static SQL_NOINLINE void
vdbeReleaseAndSetInt64(Mem * pMem, i64 val)
{
	sqlVdbeMemSetNull(pMem);
	pMem->u.i = val;
	pMem->flags = MEM_Int;
}

/*
 * Delete any previous value and set the value stored in *pMem to val,
 * manifest type INTEGER.
 */
void
sqlVdbeMemSetInt64(Mem * pMem, i64 val)
{
	if (VdbeMemDynamic(pMem)) {
		vdbeReleaseAndSetInt64(pMem, val);
	} else {
		pMem->u.i = val;
		pMem->flags = MEM_Int;
	}
}

#ifndef SQL_OMIT_FLOATING_POINT
/*
 * Delete any previous value and set the value stored in *pMem to val,
 * manifest type REAL.
 */
void
sqlVdbeMemSetDouble(Mem * pMem, double val)
{
	sqlVdbeMemSetNull(pMem);
	if (!sqlIsNaN(val)) {
		pMem->u.r = val;
		pMem->flags = MEM_Real;
	}
}
#endif

/*
 * Return true if the Mem object contains a TEXT or BLOB that is
 * too large - whose size exceeds SQL_MAX_LENGTH.
 */
int
sqlVdbeMemTooBig(Mem * p)
{
	assert(p->db != 0);
	if (p->flags & (MEM_Str | MEM_Blob)) {
		int n = p->n;
		if (p->flags & MEM_Zero) {
			n += p->u.nZero;
		}
		return n > p->db->aLimit[SQL_LIMIT_LENGTH];
	}
	return 0;
}

#ifdef SQL_DEBUG
/*
 * This routine prepares a memory cell for modification by breaking
 * its link to a shallow copy and by marking any current shallow
 * copies of this cell as invalid.
 *
 * This is used for testing and debugging only - to make sure shallow
 * copies are not misused.
 */
void
sqlVdbeMemAboutToChange(Vdbe * pVdbe, Mem * pMem)
{
	int i;
	Mem *pX;
	for (i = 0, pX = pVdbe->aMem; i < pVdbe->nMem; i++, pX++) {
		if (pX->pScopyFrom == pMem) {
			pX->flags |= MEM_Undefined;
			pX->pScopyFrom = 0;
		}
	}
	pMem->pScopyFrom = 0;
}
#endif				/* SQL_DEBUG */

/*
 * Make an shallow copy of pFrom into pTo.  Prior contents of
 * pTo are freed.  The pFrom->z field is not duplicated.  If
 * pFrom->z is used, then pTo->z points to the same thing as pFrom->z
 * and flags gets srcType (either MEM_Ephem or MEM_Static).
 */
static SQL_NOINLINE void
vdbeClrCopy(Mem * pTo, const Mem * pFrom, int eType)
{
	vdbeMemClearExternAndSetNull(pTo);
	assert(!VdbeMemDynamic(pTo));
	sqlVdbeMemShallowCopy(pTo, pFrom, eType);
}

void
sqlVdbeMemShallowCopy(Mem * pTo, const Mem * pFrom, int srcType)
{
	assert(pTo->db == pFrom->db);
	if (VdbeMemDynamic(pTo)) {
		vdbeClrCopy(pTo, pFrom, srcType);
		return;
	}
	memcpy(pTo, pFrom, MEMCELLSIZE);
	if ((pFrom->flags & MEM_Static) == 0) {
		pTo->flags &= ~(MEM_Dyn | MEM_Static | MEM_Ephem);
		assert(srcType == MEM_Ephem || srcType == MEM_Static);
		pTo->flags |= srcType;
	}
}

/*
 * Make a full copy of pFrom into pTo.  Prior contents of pTo are
 * freed before the copy is made.
 */
int
sqlVdbeMemCopy(Mem * pTo, const Mem * pFrom)
{
	int rc = SQL_OK;

	if (VdbeMemDynamic(pTo))
		vdbeMemClearExternAndSetNull(pTo);
	memcpy(pTo, pFrom, MEMCELLSIZE);
	pTo->flags &= ~MEM_Dyn;
	if (pTo->flags & (MEM_Str | MEM_Blob)) {
		if (0 == (pFrom->flags & MEM_Static)) {
			pTo->flags |= MEM_Ephem;
			rc = sqlVdbeMemMakeWriteable(pTo);
		}
	}

	return rc;
}

/*
 * Transfer the contents of pFrom to pTo. Any existing value in pTo is
 * freed. If pFrom contains ephemeral data, a copy is made.
 *
 * pFrom contains an SQL NULL when this routine returns.
 */
void
sqlVdbeMemMove(Mem * pTo, Mem * pFrom)
{
	assert(pFrom->db == 0 || pTo->db == 0 || pFrom->db == pTo->db);

	sqlVdbeMemRelease(pTo);
	memcpy(pTo, pFrom, sizeof(Mem));
	pFrom->flags = MEM_Null;
	pFrom->szMalloc = 0;
}

/*
 * Change the value of a Mem to be a string or a BLOB.
 *
 * The memory management strategy depends on the value of the xDel
 * parameter. If the value passed is SQL_TRANSIENT, then the
 * string is copied into a (possibly existing) buffer managed by the
 * Mem structure. Otherwise, any existing buffer is freed and the
 * pointer copied.
 *
 * If the string is too large (if it exceeds the SQL_LIMIT_LENGTH
 * size limit) then no memory allocation occurs.  If the string can be
 * stored without allocating memory, then it is.  If a memory allocation
 * is required to store the string, then value of pMem is unchanged.  In
 * either case, SQL_TOOBIG is returned.
 */
int
sqlVdbeMemSetStr(Mem * pMem,	/* Memory cell to set to string value */
		     const char *z,	/* String pointer */
		     int n,	/* Bytes in string, or negative */
		     u8 not_blob,	/* Encoding of z.  0 for BLOBs */
		     void (*xDel) (void *)	/* Destructor function */
    )
{
	int nByte = n;		/* New value for pMem->n */
	int iLimit;		/* Maximum allowed string or blob size */
	u16 flags = 0;		/* New value for pMem->flags */

	/* If z is a NULL pointer, set pMem to contain an SQL NULL. */
	if (!z) {
		sqlVdbeMemSetNull(pMem);
		return SQL_OK;
	}

	if (pMem->db) {
		iLimit = pMem->db->aLimit[SQL_LIMIT_LENGTH];
	} else {
		iLimit = SQL_MAX_LENGTH;
	}
	flags = (not_blob == 0 ? MEM_Blob : MEM_Str);
	if (nByte < 0) {
		assert(not_blob != 0);
		nByte = sqlStrlen30(z);
		if (nByte > iLimit)
			nByte = iLimit + 1;
		flags |= MEM_Term;
	}

	/* The following block sets the new values of Mem.z and Mem.xDel. It
	 * also sets a flag in local variable "flags" to indicate the memory
	 * management (one of MEM_Dyn or MEM_Static).
	 */
	if (xDel == SQL_TRANSIENT) {
		int nAlloc = nByte;
		if (flags & MEM_Term) {
			nAlloc += 1; //SQL_UTF8
		}
		if (nByte > iLimit) {
			return SQL_TOOBIG;
		}
		testcase(nAlloc == 0);
		testcase(nAlloc == 31);
		testcase(nAlloc == 32);
		if (sqlVdbeMemClearAndResize(pMem, MAX(nAlloc, 32))) {
			return SQL_NOMEM_BKPT;
		}
		memcpy(pMem->z, z, nAlloc);
	} else if (xDel == SQL_DYNAMIC) {
		sqlVdbeMemRelease(pMem);
		pMem->zMalloc = pMem->z = (char *)z;
		pMem->szMalloc = sqlDbMallocSize(pMem->db, pMem->zMalloc);
	} else {
		sqlVdbeMemRelease(pMem);
		pMem->z = (char *)z;
		pMem->xDel = xDel;
		flags |= ((xDel == SQL_STATIC) ? MEM_Static : MEM_Dyn);
	}

	pMem->n = nByte;
	pMem->flags = flags;

	if (nByte > iLimit) {
		return SQL_TOOBIG;
	}

	return SQL_OK;
}

/*
 * Move data out of a btree key or data field and into a Mem structure.
 * The data is payload from the entry that pCur is currently pointing
 * to.  offset and amt determine what portion of the data or key to retrieve.
 * The result is written into the pMem element.
 *
 * The pMem object must have been initialized.  This routine will use
 * pMem->zMalloc to hold the content from the btree, if possible.  New
 * pMem->zMalloc space will be allocated if necessary.  The calling routine
 * is responsible for making sure that the pMem object is eventually
 * destroyed.
 *
 * If this routine fails for any reason (malloc returns NULL or unable
 * to read from the disk) then the pMem is left in an inconsistent state.
 */
static SQL_NOINLINE int
vdbeMemFromBtreeResize(BtCursor * pCur,	/* Cursor pointing at record to retrieve. */
		       u32 offset,	/* Offset from the start of data to return bytes from. */
		       u32 amt,	/* Number of bytes to return. */
		       Mem * pMem	/* OUT: Return data in this Mem structure. */
    )
{
	int rc;
	pMem->flags = MEM_Null;
	if (SQL_OK == (rc = sqlVdbeMemClearAndResize(pMem, amt + 2))) {
		rc = sqlCursorPayload(pCur, offset, amt, pMem->z);
		if (rc == SQL_OK) {
			pMem->z[amt] = 0;
			pMem->z[amt + 1] = 0;
			pMem->flags = MEM_Blob | MEM_Term;
			pMem->n = (int)amt;
		} else {
			sqlVdbeMemRelease(pMem);
		}
	}
	return rc;
}

int
sqlVdbeMemFromBtree(BtCursor * pCur,	/* Cursor pointing at record to retrieve. */
			u32 offset,	/* Offset from the start of data to return bytes from. */
			u32 amt,	/* Number of bytes to return. */
			Mem * pMem	/* OUT: Return data in this Mem structure. */
    )
{
	char *zData;		/* Data from the btree layer */
	u32 available = 0;	/* Number of bytes available on the local btree page */
	int rc = SQL_OK;	/* Return code */

	assert(sqlCursorIsValid(pCur));
	assert(!VdbeMemDynamic(pMem));
	assert(pCur->curFlags & BTCF_TaCursor ||
	       pCur->curFlags & BTCF_TEphemCursor);


	zData = (char *)tarantoolsqlPayloadFetch(pCur, &available);
	assert(zData != 0);

	if (offset + amt <= available) {
		pMem->z = &zData[offset];
		pMem->flags = MEM_Blob | MEM_Ephem;
		pMem->n = (int)amt;
	} else {
		rc = vdbeMemFromBtreeResize(pCur, offset, amt, pMem);
	}

	return rc;
}

/*
 * The pVal argument is known to be a value other than NULL.
 * Convert it into a string with encoding enc and return a pointer
 * to a zero-terminated version of that string.
 */
static SQL_NOINLINE const void *
valueToText(sql_value * pVal)
{
	assert(pVal != 0);
	assert((pVal->flags & (MEM_Null)) == 0);
	if (pVal->flags & (MEM_Blob | MEM_Str)) {
		if (ExpandBlob(pVal))
			return 0;
		pVal->flags |= MEM_Str;
		sqlVdbeMemNulTerminate(pVal);	/* IMP: R-31275-44060 */
	} else {
		sqlVdbeMemStringify(pVal, 0);
		assert(0 == (1 & SQL_PTR_TO_INT(pVal->z)));
	}
	return pVal->z;
}

/* This function is only available internally, it is not part of the
 * external API. It works in a similar way to sql_value_text(),
 * except the data returned is in the encoding specified by the second
 * parameter, which must be one of SQL_UTF16BE, SQL_UTF16LE or
 * SQL_UTF8.
 *
 * (2006-02-16:)  The enc value can be or-ed with SQL_UTF16_ALIGNED.
 * If that is the case, then the result must be aligned on an even byte
 * boundary.
 */
const void *
sqlValueText(sql_value * pVal)
{
	if (!pVal)
		return 0;
	if ((pVal->flags & (MEM_Str | MEM_Term)) == (MEM_Str | MEM_Term)) {
		return pVal->z;
	}
	if (pVal->flags & MEM_Null) {
		return 0;
	}
	return valueToText(pVal);
}

/*
 * Create a new sql_value object.
 */
sql_value *
sqlValueNew(sql * db)
{
	Mem *p = sqlDbMallocZero(db, sizeof(*p));
	if (p) {
		p->flags = MEM_Null;
		p->db = db;
	}
	return p;
}

/*
 * Context object passed by sqlStat4ProbeSetValue() through to
 * valueNew(). See comments above valueNew() for details.
 */
struct ValueNewStat4Ctx {
	Parse *pParse;
	struct index_def *pIdx;
	UnpackedRecord **ppRec;
	int iVal;
};

/*
 * Allocate and return a pointer to a new sql_value object. If
 * the second argument to this function is NULL, the object is allocated
 * by calling sqlValueNew().
 *
 * Otherwise, if the second argument is non-zero, then this function is
 * being called indirectly by sqlStat4ProbeSetValue(). If it has not
 * already been allocated, allocate the UnpackedRecord structure that
 * that function will return to its caller here. Then return a pointer to
 * an sql_value within the UnpackedRecord.a[] array.
 */
static sql_value *
valueNew(sql * db, struct ValueNewStat4Ctx *p)
{
	if (p) {
		UnpackedRecord *pRec = p->ppRec[0];

		if (pRec == NULL) {
			struct index_def *idx = p->pIdx;
			uint32_t part_count = idx->key_def->part_count;

			int nByte = sizeof(Mem) * part_count +
				    ROUND8(sizeof(UnpackedRecord));
			pRec = (UnpackedRecord *) sqlDbMallocZero(db,
								      nByte);
			if (pRec == NULL)
				return NULL;
			pRec->key_def = key_def_dup(idx->key_def);
			if (pRec->key_def == NULL) {
				sqlDbFree(db, pRec);
				sqlOomFault(db);
				return NULL;
			}
			pRec->aMem = (Mem *)((char *) pRec +
					     ROUND8(sizeof(UnpackedRecord)));
			for (uint32_t i = 0; i < part_count; i++) {
				pRec->aMem[i].flags = MEM_Null;
				pRec->aMem[i].db = db;
			}
			p->ppRec[0] = pRec;
		}

		pRec->nField = p->iVal + 1;
		return &pRec->aMem[p->iVal];
	}

	return sqlValueNew(db);
}

/*
 * The expression object indicated by the second argument is guaranteed
 * to be a scalar SQL function. If
 *
 *   * all function arguments are SQL literals,
 *   * one of the SQL_FUNC_CONSTANT or _SLOCHNG function flags is set, and
 *   * the SQL_FUNC_NEEDCOLL function flag is not set,
 *
 * then this routine attempts to invoke the SQL function. Assuming no
 * error occurs, output parameter (*ppVal) is set to point to a value
 * object containing the result before returning SQL_OK.
 *
 * Type @type is applied to the result of the function before returning.
 * If the result is a text value, the sql_value object uses encoding
 * enc.
 *
 * If the conditions above are not met, this function returns SQL_OK
 * and sets (*ppVal) to NULL. Or, if an error occurs, (*ppVal) is set to
 * NULL and an sql error code returned.
 */
static int
valueFromFunction(sql * db,	/* The database connection */
		  Expr * p,	/* The expression to evaluate */
		  enum field_type type,
		  sql_value ** ppVal,	/* Write the new value here */
		  struct ValueNewStat4Ctx *pCtx	/* Second argument for valueNew() */
    )
{
	sql_context ctx;	/* Context object for function invocation */
	sql_value **apVal = 0;	/* Function arguments */
	int nVal = 0;		/* Size of apVal[] array */
	FuncDef *pFunc = 0;	/* Function definition */
	sql_value *pVal = 0;	/* New value */
	int rc = SQL_OK;	/* Return code */
	ExprList *pList = 0;	/* Function arguments */
	int i;			/* Iterator variable */

	assert(pCtx != 0);
	assert((p->flags & EP_TokenOnly) == 0);
	pList = p->x.pList;
	if (pList)
		nVal = pList->nExpr;
	pFunc = sqlFindFunction(db, p->u.zToken, nVal, 0);
	assert(pFunc);
	if ((pFunc->funcFlags & (SQL_FUNC_CONSTANT | SQL_FUNC_SLOCHNG)) ==
	    0 || (pFunc->funcFlags & SQL_FUNC_NEEDCOLL)
	    ) {
		return SQL_OK;
	}

	if (pList) {
		apVal =
		    (sql_value **) sqlDbMallocZero(db,
							   sizeof(apVal[0]) *
							   nVal);
		if (apVal == 0) {
			rc = SQL_NOMEM_BKPT;
			goto value_from_function_out;
		}
		for (i = 0; i < nVal; i++) {
			rc = sqlValueFromExpr(db, pList->a[i].pExpr,
						  type, &apVal[i]);
			if (apVal[i] == 0 || rc != SQL_OK)
				goto value_from_function_out;
		}
	}

	pVal = valueNew(db, pCtx);
	if (pVal == 0) {
		rc = SQL_NOMEM_BKPT;
		goto value_from_function_out;
	}

	assert(pCtx->pParse->rc == SQL_OK);
	memset(&ctx, 0, sizeof(ctx));
	ctx.pOut = pVal;
	ctx.pFunc = pFunc;
	pFunc->xSFunc(&ctx, nVal, apVal);
	if (ctx.isError) {
		rc = ctx.isError;
		sqlErrorMsg(pCtx->pParse, "%s", sql_value_text(pVal));
	} else {
		sql_value_apply_type(pVal, type);
		assert(rc == SQL_OK);
	}
	pCtx->pParse->rc = rc;

 value_from_function_out:
	if (rc != SQL_OK) {
		pVal = 0;
	}
	if (apVal) {
		for (i = 0; i < nVal; i++) {
			sqlValueFree(apVal[i]);
		}
		sqlDbFree(db, apVal);
	}

	*ppVal = pVal;
	return rc;
}

/*
 * Extract a value from the supplied expression in the manner described
 * above sqlValueFromExpr(). Allocate the sql_value object
 * using valueNew().
 *
 * If pCtx is NULL and an error occurs after the sql_value object
 * has been allocated, it is freed before returning. Or, if pCtx is not
 * NULL, it is assumed that the caller will free any allocated object
 * in all cases.
 */
static int
valueFromExpr(sql * db,	/* The database connection */
	      Expr * pExpr,	/* The expression to evaluate */
	      enum field_type type,
	      sql_value ** ppVal,	/* Write the new value here */
	      struct ValueNewStat4Ctx *pCtx	/* Second argument for valueNew() */
    )
{
	int op;
	char *zVal = 0;
	sql_value *pVal = 0;
	int negInt = 1;
	const char *zNeg = "";
	int rc = SQL_OK;

	assert(pExpr != 0);
	while ((op = pExpr->op) == TK_UPLUS || op == TK_SPAN)
		pExpr = pExpr->pLeft;
	if (NEVER(op == TK_REGISTER))
		op = pExpr->op2;

	/* Compressed expressions only appear when parsing the DEFAULT clause
	 * on a table column definition, and hence only when pCtx==0.  This
	 * check ensures that an EP_TokenOnly expression is never passed down
	 * into valueFromFunction().
	 */
	assert((pExpr->flags & EP_TokenOnly) == 0 || pCtx == 0);

	if (op == TK_CAST) {
		rc = valueFromExpr(db, pExpr->pLeft, pExpr->type, ppVal, pCtx);
		testcase(rc != SQL_OK);
		if (*ppVal) {
			sqlVdbeMemCast(*ppVal, pExpr->type);
			sql_value_apply_type(*ppVal, type);
		}
		return rc;
	}

	/* Handle negative integers in a single step.  This is needed in the
	 * case when the value is -9223372036854775808.
	 */
	if (op == TK_UMINUS
	    && (pExpr->pLeft->op == TK_INTEGER
		|| pExpr->pLeft->op == TK_FLOAT)) {
		pExpr = pExpr->pLeft;
		op = pExpr->op;
		negInt = -1;
		zNeg = "-";
	}

	if (op == TK_STRING || op == TK_FLOAT || op == TK_INTEGER) {
		pVal = valueNew(db, pCtx);
		if (pVal == 0)
			goto no_mem;
		if (ExprHasProperty(pExpr, EP_IntValue)) {
			sqlVdbeMemSetInt64(pVal,
					       (i64) pExpr->u.iValue * negInt);
		} else {
			zVal =
			    sqlMPrintf(db, "%s%s", zNeg, pExpr->u.zToken);
			if (zVal == 0)
				goto no_mem;
			sqlValueSetStr(pVal, -1, zVal, SQL_DYNAMIC);
		}
		if ((op == TK_INTEGER || op == TK_FLOAT) &&
		    type == FIELD_TYPE_SCALAR) {
			sql_value_apply_type(pVal, FIELD_TYPE_NUMBER);
		} else {
			sql_value_apply_type(pVal, type);
		}
		if (pVal->flags & (MEM_Int | MEM_Real))
			pVal->flags &= ~MEM_Str;
	} else if (op == TK_UMINUS) {
		/* This branch happens for multiple negative signs.  Ex: -(-5) */
		if (SQL_OK ==
		    sqlValueFromExpr(db, pExpr->pLeft, type, &pVal)
		    && pVal != 0) {
			if ((rc = sqlVdbeMemNumerify(pVal)) != SQL_OK)
				return rc;
			if (pVal->flags & MEM_Real) {
				pVal->u.r = -pVal->u.r;
			} else if (pVal->u.i == SMALLEST_INT64) {
				pVal->u.r = -(double)SMALLEST_INT64;
				MemSetTypeFlag(pVal, MEM_Real);
			} else {
				pVal->u.i = -pVal->u.i;
			}
			sql_value_apply_type(pVal, type);
		}
	} else if (op == TK_NULL) {
		pVal = valueNew(db, pCtx);
		if (pVal == 0)
			goto no_mem;
		if ((rc = sqlVdbeMemNumerify(pVal)) != SQL_OK)
			return rc;
	}
#ifndef SQL_OMIT_BLOB_LITERAL
	else if (op == TK_BLOB) {
		int nVal;
		assert(pExpr->u.zToken[0] == 'x' || pExpr->u.zToken[0] == 'X');
		assert(pExpr->u.zToken[1] == '\'');
		pVal = valueNew(db, pCtx);
		if (!pVal)
			goto no_mem;
		zVal = &pExpr->u.zToken[2];
		nVal = sqlStrlen30(zVal) - 1;
		assert(zVal[nVal] == '\'');
		sqlVdbeMemSetStr(pVal, sqlHexToBlob(db, zVal, nVal),
				     nVal / 2, 0, SQL_DYNAMIC);
	}
#endif

	else if (op == TK_FUNCTION && pCtx != 0) {
		rc = valueFromFunction(db, pExpr, type, &pVal, pCtx);
	}

	*ppVal = pVal;
	return rc;

 no_mem:
	sqlOomFault(db);
	sqlDbFree(db, zVal);
	assert(*ppVal == 0);
	if (pCtx == 0)
		sqlValueFree(pVal);

	return SQL_NOMEM_BKPT;
}

/*
 * Create a new sql_value object, containing the value of pExpr.
 *
 * This only works for very simple expressions that consist of one constant
 * token (i.e. "5", "5.1", "'a string'"). If the expression can
 * be converted directly into a value, then the value is allocated and
 * a pointer written to *ppVal. The caller is responsible for deallocating
 * the value by passing it to sqlValueFree() later on. If the expression
 * cannot be converted to a value, then *ppVal is set to NULL.
 */
int
sqlValueFromExpr(sql * db,	/* The database connection */
		     Expr * pExpr,	/* The expression to evaluate */
		     enum field_type type,
		     sql_value ** ppVal	/* Write the new value here */
    )
{
	return pExpr ? valueFromExpr(db, pExpr, type, ppVal, 0) : 0;
}

/*
 * The implementation of the sql_record() function. This function accepts
 * a single argument of any type. The return value is a formatted database
 * record (a blob) containing the argument value.
 *
 * This is used to convert the value stored in the 'sample' column of the
 * sql_stat4 table to the record format sql uses internally.
 */
static void
recordFunc(sql_context * context, int argc, sql_value ** argv)
{
	const int file_format = 1;
	u32 iSerial;		/* Serial type */
	int nSerial;		/* Bytes of space for iSerial as varint */
	u32 nVal;		/* Bytes of space required for argv[0] */
	int nRet;
	sql *db;
	u8 *aRet;

	UNUSED_PARAMETER(argc);
	iSerial = sqlVdbeSerialType(argv[0], file_format, &nVal);
	nSerial = sqlVarintLen(iSerial);
	db = sql_context_db_handle(context);

	nRet = 1 + nSerial + nVal;
	aRet = sqlDbMallocRawNN(db, nRet);
	if (aRet == 0) {
		sql_result_error_nomem(context);
	} else {
		aRet[0] = nSerial + 1;
		putVarint32(&aRet[1], iSerial);
		sqlVdbeSerialPut(&aRet[1 + nSerial], argv[0], iSerial);
		sql_result_blob(context, aRet, nRet, SQL_TRANSIENT);
		sqlDbFree(db, aRet);
	}
}

/*
 * Register built-in functions used to help read ANALYZE data.
 */
void
sqlAnalyzeFunctions(void)
{
	static FuncDef aAnalyzeTableFuncs[] = {
		FUNCTION(sql_record, 1, 0, 0, recordFunc, 0),
	};
	sqlInsertBuiltinFuncs(aAnalyzeTableFuncs,
				  ArraySize(aAnalyzeTableFuncs));
}

/*
 * Attempt to extract a value from pExpr and use it to construct *ppVal.
 *
 * If pAlloc is not NULL, then an UnpackedRecord object is created for
 * pAlloc if one does not exist and the new value is added to the
 * UnpackedRecord object.
 *
 * A value is extracted in the following cases:
 *
 *  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
 *
 *  * The expression is a bound variable, and this is a reprepare, or
 *
 *  * The expression is a literal value.
 *
 * On success, *ppVal is made to point to the extracted value.  The caller
 * is responsible for ensuring that the value is eventually freed.
 */
static int
stat4ValueFromExpr(Parse * pParse,	/* Parse context */
		   Expr * pExpr,	/* The expression to extract a value from */
		   enum field_type type,
		   struct ValueNewStat4Ctx *pAlloc,	/* How to allocate space.  Or NULL */
		   sql_value ** ppVal	/* OUT: New value object (or NULL) */
    )
{
	int rc = SQL_OK;
	sql_value *pVal = 0;
	sql *db = pParse->db;

	/* Skip over any TK_COLLATE nodes */
	pExpr = sqlExprSkipCollate(pExpr);

	if (!pExpr) {
		pVal = valueNew(db, pAlloc);
		if (pVal) {
			sqlVdbeMemSetNull((Mem *) pVal);
		}
	} else if (pExpr->op == TK_VARIABLE
		   || NEVER(pExpr->op == TK_REGISTER
			    && pExpr->op2 == TK_VARIABLE)
	    ) {
		Vdbe *v;
		int iBindVar = pExpr->iColumn;
		sqlVdbeSetVarmask(pParse->pVdbe, iBindVar);
		if ((v = pParse->pReprepare) != 0) {
			pVal = valueNew(db, pAlloc);
			if (pVal) {
				rc = sqlVdbeMemCopy((Mem *) pVal,
							&v->aVar[iBindVar - 1]);
				if (rc == SQL_OK) {
					sql_value_apply_type(pVal, type);
				}
				pVal->db = pParse->db;
			}
		}
	} else {
		rc = valueFromExpr(db, pExpr, type, &pVal, pAlloc);
	}

	assert(pVal == 0 || pVal->db == db);
	*ppVal = pVal;
	return rc;
}

/*
 * This function is used to allocate and populate UnpackedRecord
 * structures intended to be compared against sample index keys stored
 * in the sql_stat4 table.
 *
 * A single call to this function populates zero or more fields of the
 * record starting with field iVal (fields are numbered from left to
 * right starting with 0). A single field is populated if:
 *
 *  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
 *
 *  * The expression is a bound variable, and this is a reprepare, or
 *
 *  * The sqlValueFromExpr() function is able to extract a value
 *    from the expression (i.e. the expression is a literal value).
 *
 * Or, if pExpr is a TK_VECTOR, one field is populated for each of the
 * vector components that match either of the two latter criteria listed
 * above.
 *
 * Before any value is appended to the record, the type of the
 * corresponding column within index pIdx is applied to it. Before
 * this function returns, output parameter *pnExtract is set to the
 * number of values appended to the record.
 *
 * When this function is called, *ppRec must either point to an object
 * allocated by an earlier call to this function, or must be NULL. If it
 * is NULL and a value can be successfully extracted, a new UnpackedRecord
 * is allocated (and *ppRec set to point to it) before returning.
 *
 * Unless an error is encountered, SQL_OK is returned. It is not an
 * error if a value cannot be extracted from pExpr. If an error does
 * occur, an sql error code is returned.
 */
int
sqlStat4ProbeSetValue(Parse * pParse,	/* Parse context */
			  struct index_def *idx,
			  UnpackedRecord ** ppRec,	/* IN/OUT: Probe record */
			  Expr * pExpr,	/* The expression to extract a value from */
			  int nElem,	/* Maximum number of values to append */
			  int iVal,	/* Array element to populate */
			  int *pnExtract	/* OUT: Values appended to the record */
    )
{
	int rc = SQL_OK;
	int nExtract = 0;

	if (pExpr == 0 || pExpr->op != TK_SELECT) {
		int i;
		struct ValueNewStat4Ctx alloc;

		alloc.pParse = pParse;
		alloc.pIdx = idx;
		alloc.ppRec = ppRec;

		for (i = 0; i < nElem; i++) {
			sql_value *pVal = 0;
			Expr *pElem =
			    (pExpr ? sqlVectorFieldSubexpr(pExpr, i) : 0);
			enum field_type type =
				idx->key_def->parts[iVal + i].type;
			alloc.iVal = iVal + i;
			rc = stat4ValueFromExpr(pParse, pElem, type, &alloc,
						&pVal);
			if (!pVal)
				break;
			nExtract++;
		}
	}

	*pnExtract = nExtract;
	return rc;
}

/*
 * Attempt to extract a value from expression pExpr using the methods
 * as described for sqlStat4ProbeSetValue() above.
 *
 * If successful, set *ppVal to point to a new value object and return
 * SQL_OK. If no value can be extracted, but no other error occurs
 * (e.g. OOM), return SQL_OK and set *ppVal to NULL. Or, if an error
 * does occur, return an sql error code. The final value of *ppVal
 * is undefined in this case.
 */
int
sqlStat4ValueFromExpr(Parse * pParse,	/* Parse context */
			  Expr * pExpr,	/* The expression to extract a value from */
			  enum field_type type,
			  sql_value ** ppVal	/* OUT: New value object (or NULL) */
    )
{
	return stat4ValueFromExpr(pParse, pExpr, type, 0, ppVal);
}

int
sql_stat4_column(struct sql *db, const char *record, uint32_t col_num,
		 sql_value **res)
{
	/* Write result into this Mem object. */
	struct Mem *mem = *res;
	const char *a = record;
	assert(mp_typeof(a[0]) == MP_ARRAY);
	uint32_t col_cnt = mp_decode_array(&a);
	(void) col_cnt;
	assert(col_cnt > col_num);
	for (uint32_t i = 0; i < col_num; i++)
		mp_next(&a);
	if (mem == NULL) {
		mem = sqlValueNew(db);
		*res = mem;
		if (mem == NULL) {
			diag_set(OutOfMemory, sizeof(struct Mem),
				 "sqlValueNew", "mem");
			return -1;
		}
	}
	sqlVdbeMsgpackGet((const unsigned char *) a, mem);
	return 0;
}

/*
 * Unless it is NULL, the argument must be an UnpackedRecord object returned
 * by an earlier call to sqlStat4ProbeSetValue(). This call deletes
 * the object.
 */
void
sqlStat4ProbeFree(UnpackedRecord * pRec)
{
	if (pRec != NULL) {
		int part_count = pRec->key_def->part_count;
		struct Mem *aMem = pRec->aMem;
		for (int i = 0; i < part_count; i++)
			sqlVdbeMemRelease(&aMem[i]);
		sqlDbFree(aMem[0].db, pRec);
	}
}

/*
 * Change the string value of an sql_value object
 */
void
sqlValueSetStr(sql_value * v,	/* Value to be set */
		   int n,	/* Length of string z */
		   const void *z,	/* Text of the new string */
		   void (*xDel) (void *)	/* Destructor for the string */
    )
{
	if (v)
		sqlVdbeMemSetStr((Mem *) v, z, n, 1, xDel);
}

/*
 * Free an sql_value object
 */
void
sqlValueFree(sql_value * v)
{
	if (!v)
		return;
	sqlVdbeMemRelease((Mem *) v);
	sqlDbFree(((Mem *) v)->db, v);
}

/*
 * The sqlValueBytes() routine returns the number of bytes in the
 * sql_value object assuming that it uses the encoding "enc".
 * The valueBytes() routine is a helper function.
 */
static SQL_NOINLINE int
valueBytes(sql_value * pVal)
{
	return valueToText(pVal) != 0 ? pVal->n : 0;
}

int
sqlValueBytes(sql_value * pVal)
{
	Mem *p = (Mem *) pVal;
	assert((p->flags & MEM_Null) == 0
	       || (p->flags & (MEM_Str | MEM_Blob)) == 0);
	if ((p->flags & MEM_Str) != 0) {
		return p->n;
	}
	if ((p->flags & MEM_Blob) != 0) {
		if (p->flags & MEM_Zero) {
			return p->n + p->u.nZero;
		} else {
			return p->n;
		}
	}
	if (p->flags & MEM_Null)
		return 0;
	return valueBytes(pVal);
}

void
mpstream_encode_vdbe_mem(struct mpstream *stream, struct Mem *var)
{
	assert(memIsValid(var));
	int64_t i;
	if (var->flags & MEM_Null) {
		mpstream_encode_nil(stream);
	} else if (var->flags & MEM_Real) {
		/*
		 * We can't pass to INT iterator float
		 * value. Hence, if floating point value
		 * lacks fractional component, we can
		 * encode it as INT and successfully
		 * pass to INT iterator.
		 */
		i = var->u.r;
		if (i == var->u.r)
			goto encode_int;
		mpstream_encode_double(stream, var->u.r);
	} else if (var->flags & MEM_Int) {
		i = var->u.i;
encode_int:
		if (var->u.i >= 0)
			mpstream_encode_uint(stream, i);
		else
			mpstream_encode_int(stream, i);
	} else if (var->flags & MEM_Str) {
		mpstream_encode_strn(stream, var->z, var->n);
	} else if (var->flags & MEM_Bool) {
		mpstream_encode_bool(stream, var->u.b);
	} else {
		/*
		 * Emit BIN header iff the BLOB doesn't store
		 * MsgPack content.
		 */
		if ((var->flags & MEM_Subtype) == 0 ||
		     var->subtype != SQL_SUBTYPE_MSGPACK) {
			uint32_t binl = var->n +
					((var->flags & MEM_Zero) ?
					var->u.nZero : 0);
			mpstream_encode_binl(stream, binl);
		}
		mpstream_memcpy(stream, var->z, var->n);
		if (var->flags & MEM_Zero)
			mpstream_memset(stream, 0, var->u.nZero);
	}
}

char *
sql_vdbe_mem_encode_tuple(struct Mem *fields, uint32_t field_count,
			  uint32_t *tuple_size, struct region *region)
{
	size_t used = region_used(region);
	bool is_error = false;
	struct mpstream stream;
	mpstream_init(&stream, region, region_reserve_cb, region_alloc_cb,
		      set_encode_error, &is_error);
	mpstream_encode_array(&stream, field_count);
	for (struct Mem *field = fields; field < fields + field_count; field++)
		mpstream_encode_vdbe_mem(&stream, field);
	mpstream_flush(&stream);
	if (is_error) {
		diag_set(OutOfMemory, stream.pos - stream.buf,
			 "mpstream_flush", "stream");
		return NULL;
	}
	*tuple_size = region_used(region) - used;
	char *tuple = region_join(region, *tuple_size);
	if (tuple == NULL) {
		diag_set(OutOfMemory, *tuple_size, "region_join", "tuple");
		return NULL;
	}
	mp_tuple_assert(tuple, tuple + *tuple_size);
	return tuple;
}
