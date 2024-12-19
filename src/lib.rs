use std::cmp;

use pg_sys::Datum;
use pgrx::{prelude::*, varlena, Internal, PgMemoryContexts};

mod gucs;

::pgrx::pg_module_magic!();
extension_sql_file!("../sql/pg_bigmr--0.1.0.sql", name = "pg_bigmr", finalize);

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
        // TODO: Return NULL
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

    let mut result: Vec<String> = Vec::new();
    let padded_str = format!(" {} ", input);

    if input.is_empty() {
        return result;
    };

    result = make_bigrams(padded_str);
    remove_duplicate_bigms(result)
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

// TODO
#[pg_extern(immutable, parallel_safe, strict)]
fn gin_extract_query_bigm(
    _query: &str,
    _nkeys: Internal,
    _strategy_number: i16,
    _pmatch: Internal,
    _extra_data: Internal,
    _null_flags: Internal,
    _search_mode: Internal,
) -> Internal {
    Internal::new(0)
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

// TODO
#[pg_extern(immutable, parallel_safe, strict)]
fn gin_bigm_triconsistent(
    _check: Internal,
    _strategy_number: i16,
    _query: &str,
    _nkeys: i32,
    _extra_data: Internal,
    _query_keys: Internal,
    _null_flags: Internal,
) -> i8 {
    0
}

// Adds bigrams from words (already padded).
fn make_bigrams(padded_str: String) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    let mut chars = padded_str.chars().peekable();
    while let Some(c) = chars.next() {
        if chars.peek().is_none() {
            break;
        } else {
            result.push(format!("{}{}", c, chars.peek().unwrap()));
        }
    }
    result
}

fn remove_duplicate_bigms(mut duplicated_bigms: Vec<String>) -> Vec<String> {
    duplicated_bigms.sort();
    duplicated_bigms.dedup();
    duplicated_bigms
}
