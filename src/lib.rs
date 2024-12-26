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

    if input.is_empty() {
        return Vec::new();
    };

    let bigram_list = BigramList::from_value(input);
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
    _pmatch: Internal,
    _extra_data: Internal,
    _null_flags: Internal,
    search_mode: Internal,
) -> Internal {
    let bigram_list;
    let bgmlen: i32;

    match strategy_number {
        LIKE_STRATEGY_NUMBER => {
            let recheck;

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

            unsafe {
                // let a = PgMemoryContexts::CurrentMemoryContext.palloc0(mem::size_of::<bool>()) as *mut bool;
                // let extra_data_ptr =
                //     PgBox::from_pg(extra_data.get_mut().unwrap() as *mut bool);
                let extra_data_ptr = PgMemoryContexts::CurrentMemoryContext
                    .palloc0(mem::size_of::<bool>())
                    as *mut bool;
                recheck = extra_data_ptr;
            };

            // TODO
            if bgmlen == 1 && !bigram_list.removed_dups {
                unsafe { *recheck = false };
            } else {
                unsafe { *recheck = true };
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
        entries[i] = Datum::from(s_varlena);

        // TODO: Partial match
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

            // let extra_data_value = unsafe { *(extra_data.get().unwrap() as *const bool) };
            let extra_data_value = extra_data.unwrap().is_some();
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
