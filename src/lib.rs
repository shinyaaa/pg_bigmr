use std::{cmp, mem};

use bigram::BigramList;
use pg_sys::Datum;
use pgrx::{prelude::*, varlena, Internal, PgMemoryContexts};

mod bigram;
mod gucs;

::pgrx::pg_module_magic!();
extension_sql_file!("../sql/pg_bigmr--0.1.0.sql", name = "pg_bigmr", finalize);

// operator strategy numbers
const LIKE_STRATEGY_NUMBER: i16 = 1;
const SIMILARITY_STRATEGY_NUMBER: i16 = 2;

// Page numbers of fixed-location pages
const GIN_METAPAGE_BLKNO: u32 = 0;

// Macros for buffer lock/unlock operations
const GIN_SHARE: i32 = 1;

#[pg_guard]
pub extern "C" fn _PG_init() {
    self::gucs::init();
}

#[pg_operator(parallel_safe, stable, strict)]
#[opname(=%)]
#[restrict(contsel)]
#[join(contjoinsel)]
fn bigm_similarity_op(input1: &str, input2: &str) -> bool {
    let similarity = bigm_similarity(input1, input2);
    similarity as f64 >= gucs::similarity_limit()
}

#[pg_extern(immutable, parallel_safe, strict)]
fn likequery(query: &str) -> String {
    if query.is_empty() {
        // TODO:
        // Due to the specifications of pgrx, NULL cannot be returned,
        // so it results in an error.
        pgrx::error!("query cannot be empty");
    };

    // TODO: Handle OOM errors for large queries.

    let result = query
        .replace(r"\", r"\\")
        .replace("%", r"\%")
        .replace("_", r"\_");
    format!("%{}%", result)
}

#[pg_extern(immutable, parallel_safe, strict)]
fn show_bigm(input: &str) -> Vec<String> {
    // TODO: Handle OOM errors for large queries.
    let trimed_input = input.trim();
    if trimed_input.is_empty() {
        return Vec::new();
    };
    let bigram_list = BigramList::from_value(trimed_input);
    bigram_list.bigrams
}

#[pg_extern(immutable, parallel_safe, strict)]
fn bigm_similarity(input1: &str, input2: &str) -> f32 {
    // explicit test is needed to avoid 0/0 division when both lengths are 0
    if input1.chars().count() == 0 || input2.chars().count() == 0 {
        return 0.0;
    };

    let mut count = 0;
    let bigm1 = show_bigm(input1);
    let bigm2 = show_bigm(input2);

    let mut bigm1_iter = bigm1.iter();
    let mut bigm2_iter = bigm2.iter();

    let mut b1 = bigm1_iter.next();
    let mut b2 = bigm2_iter.next();

    while b1.is_some() && b2.is_some() {
        match b1.cmp(&b2) {
            cmp::Ordering::Less => {
                b1 = bigm1_iter.next();
            }
            cmp::Ordering::Greater => {
                b2 = bigm2_iter.next();
            }
            cmp::Ordering::Equal => {
                b1 = bigm1_iter.next();
                b2 = bigm2_iter.next();
                count += 1;
            }
        }
    }

    let max_len = if bigm1.len() > bigm2.len() {
        bigm1.len()
    } else {
        bigm2.len()
    };
    count as f32 / max_len as f32
}

#[pg_extern(immutable, parallel_safe, strict)]
fn pg_gin_pending_stats(
    index_oid: pg_sys::Oid,
) -> TableIterator<'static, (name!(pages, i32), name!(tuples, i64))> {
    unsafe {
        let index_rel = PgBox::from_pg(pg_sys::relation_open(
            index_oid,
            pg_sys::AccessShareLock as _,
        ));
        let pg_class_entry = PgBox::from_pg(index_rel.rd_rel);

        if pg_class_entry.relkind != pg_sys::RELKIND_INDEX as i8
            || pg_class_entry.relam != pg_sys::GIN_AM_OID
        {
            pgrx::error!(
                "relation \"{}\" is not a GIN index",
                name_data_to_str(&pg_class_entry.relname)
            );
        };

        // Reject attempts to read non-local temporary relations; we would be
        // likely to get wrong data since we have no visibility into the owning
        // session's local buffers.
        if pg_class_entry.relpersistence == 't' as i8 && !index_rel.rd_islocaltemp {
            pgrx::error!("cannot access temporary indexes of other sessions");
        };

        // Obtain statistic information from the meta page
        let metabuffer = pg_sys::ReadBuffer(index_rel.as_ptr(), GIN_METAPAGE_BLKNO);
        pg_sys::LockBuffer(metabuffer, GIN_SHARE);
        let metapage = pg_sys::BufferGetPage(metabuffer) as *const u8;

        // pgrx cannot use GinPageGetMeta, so directly access the GIN meta page.
        // Bytes 36-39 of the page indicate the number of GIN pending pages.
        let n_pending_pages_byte = std::slice::from_raw_parts(metapage.add(36), 4);
        let n_pending_pages = i32::from_ne_bytes(n_pending_pages_byte.try_into().unwrap());

        // Bytes 40-47 of the page indicate the number of GIN pending tuples.
        let n_pending_tuples_byte = std::slice::from_raw_parts(metapage.add(40), 8);
        let n_pending_tuples = i64::from_ne_bytes(n_pending_tuples_byte.try_into().unwrap());

        pg_sys::UnlockReleaseBuffer(metabuffer);
        pg_sys::relation_close(index_rel.as_ptr(), pg_sys::AccessShareLock as _);

        TableIterator::new(vec![(n_pending_pages, n_pending_tuples)])
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
fn bigmtextcmp(input1: &str, input2: &str) -> i32 {
    match input1.cmp(input2) {
        cmp::Ordering::Equal => 0,
        cmp::Ordering::Less => -1,
        cmp::Ordering::Greater => 1,
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
fn gin_extract_value_bigm(item_value: &str, nkeys: Internal) -> Internal {
    let bigrams = show_bigm(item_value);
    let bgmlen = bigrams.len();

    unsafe {
        let mut nkeys_ptr = PgBox::from_pg(nkeys.get_mut().unwrap() as *mut i32);
        *nkeys_ptr = bgmlen as i32;
    };

    let datums = unsafe {
        PgMemoryContexts::CurrentMemoryContext.palloc0_slice::<pg_sys::Datum>(bgmlen as usize)
    };

    for (i, bgm) in bigrams.iter().enumerate() {
        let s_varlena = varlena::rust_str_to_text_p(bgm).into_pg();
        datums[i] = Datum::from(s_varlena);
    }

    Internal::from(Some(Datum::from(datums.as_mut_ptr())))
}

#[pg_extern(immutable, parallel_safe, strict)]
fn gin_extract_query_bigm(
    query: &str,
    nkeys: Internal,
    strategy_number: i16,
    mut pmatch: Internal,
    extra_data: Internal,
    _null_flags: Internal,
    search_mode: Internal,
) -> Internal {
    let bigram_list;
    let bgmlen: i32;

    match strategy_number {
        LIKE_STRATEGY_NUMBER => {
            // For wildcard search we extract all the bigrams that every
            // potentially-matching string must include.
            bigram_list = BigramList::from_query(query);
            bgmlen = bigram_list.bigrams.len() as i32;

            // Check whether the heap tuple fetched by index search needs to
            // be rechecked against the query. If the search word consists of
            // one or two characters and doesn't contain any space character,
            // we can guarantee that the index test would be exact. That is,
            // the heap tuple does match the query, so it doesn't need to be
            // rechecked.
            let needs_recheck = !(bgmlen == 1 && !bigram_list.removed_dups && !query.contains(' '));
            unsafe {
                let recheck = PgMemoryContexts::CurrentMemoryContext.palloc0(mem::size_of::<bool>())
                    as *mut bool;
                *recheck = needs_recheck;
                *extra_data.get_mut().unwrap() = recheck as *const _;
            }
        }
        SIMILARITY_STRATEGY_NUMBER => {
            bigram_list = BigramList::from_value(query);
            bgmlen = bigram_list.bigrams.len() as i32;
        }
        _ => {
            pgrx::error!("unrecognized strategy number: {strategy_number}");
        }
    }

    let nkeys_ = if gucs::gin_key_limit() == 0 {
        bgmlen
    } else {
        cmp::min(gucs::gin_key_limit(), bgmlen)
    };
    unsafe {
        let mut nkeys_ptr = PgBox::from_pg(nkeys.get_mut().unwrap() as *mut i32);
        *nkeys_ptr = nkeys_;
    };

    // Convert String to varlena
    let entries = unsafe {
        let size = mem::size_of::<pg_sys::Datum>() * bgmlen as usize;
        PgMemoryContexts::CurrentMemoryContext.palloc0_slice::<pg_sys::Datum>(size)
    };
    for (i, bgm) in bigram_list.bigrams.iter().enumerate() {
        let s_varlena = varlena::rust_str_to_text_p(bgm).into_pg();
        entries[i] = Datum::from(s_varlena)
    }

    // Partial match
    if bigram_list.pmatch {
        let mut pmatch_: &mut [bool] = &mut [];
        if !pmatch.initialized() {
            pmatch_ = unsafe {
                let size = mem::size_of::<bool>() * bgmlen as usize;
                PgMemoryContexts::CurrentMemoryContext.palloc0_slice::<bool>(size)
            };
        }
        pmatch_
            .iter_mut()
            .take(nkeys_ as usize)
            .for_each(|item| *item = true);
        unsafe { pmatch.insert(Internal::new(pmatch_)) };
    }

    // If no bigram was extracted then we have to scan all the index.
    if nkeys_ == 0 {
        unsafe {
            let mut search_mode_ptr = PgBox::from_pg(search_mode.get_mut().unwrap() as *mut u32);
            *search_mode_ptr = pg_sys::GIN_SEARCH_MODE_ALL
        }
    }

    Internal::from(Some(Datum::from(entries.as_mut_ptr())))
}

// TODO
#[allow(clippy::too_many_arguments)]
#[pg_extern(immutable, parallel_safe, strict)]
fn gin_bigm_consistent(
    _check: Internal,
    _strategy_number: i16,
    _query: &str,
    _nkeys: i32,
    _extra_data: Internal,
    _recheck: Internal,
    _query_keys: Internal,
    _null_flags: Internal,
) -> bool {
    true
}

#[pg_extern(immutable, parallel_safe, strict)]
fn gin_bigm_compare_partial(
    input1: &str,
    input2: &str,
    _strategy_number: i16,
    _extra_data: Internal,
) -> i32 {
    match input1 == input2 {
        true => 0,
        false => 1,
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
fn gin_bigm_triconsistent(
    check: Internal,
    strategy_number: i16,
    _query: &str,
    nkeys: i32,
    extra_data: Internal,
    _query_keys: Internal,
    _null_flags: Internal,
) -> pg_sys::GinTernaryValue {
    let check_ = unsafe { check.get().unwrap() as *const pg_sys::GinTernaryValue };
    let check_ = unsafe { std::slice::from_raw_parts(check_, nkeys as usize) };
    let mut res;

    match strategy_number {
        LIKE_STRATEGY_NUMBER => {
            // Don't recheck the heap tuple against the query if either
            // pg_bigmr.enable_recheck is disabled or the search word is the
            // special one so that the index can return the exact result.

            let extra_data_value = unsafe { *(extra_data.get().unwrap() as *const bool) };
            res = if gucs::enable_recheck() && (extra_data_value || nkeys != 1) {
                pg_sys::GIN_MAYBE
            } else {
                pg_sys::GIN_TRUE
            };

            // Check if all extracted bigrams are presented.
            for chk in check_.iter().take(nkeys as usize) {
                if *chk == pg_sys::GIN_FALSE as i8 {
                    return pg_sys::GIN_FALSE as i8;
                };
            }
        }
        SIMILARITY_STRATEGY_NUMBER => {
            // Count the matches
            let mut ntrue = 0;
            for chk in check_.iter().take(nkeys as usize) {
                if *chk != pg_sys::GIN_FALSE as i8 {
                    ntrue += 1;
                };
            }

            // See comment in gin_bigm_consistent() about upper bound formula
            res = if nkeys == 0 {
                pg_sys::GIN_FALSE
            } else if ntrue as f32 / nkeys as f32 >= gucs::similarity_limit() as f32 {
                pg_sys::GIN_MAYBE
            } else {
                pg_sys::GIN_FALSE
            };

            if res != pg_sys::GIN_FALSE && !gucs::enable_recheck() {
                res = pg_sys::GIN_TRUE
            }
        }
        _ => {
            pgrx::error!("unrecognized strategy number: {strategy_number}");
        }
    }
    res as pg_sys::GinTernaryValue
}
