use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

static ENABLE_RECHECK: GucSetting<bool> = GucSetting::<bool>::new(true);
static GIN_KEY_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(0);
static SIMILARITY_LIMIT: GucSetting<f64> = GucSetting::<f64>::new(0.3);
static BIGM_LAST_UPDATE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"2024.06.06"));

pub fn init() {
    GucRegistry::define_bool_guc(
        "pg_bigmr.enable_recheck",
        "Recheck that heap tuples fetched from index match the query.",
        "", // TODO: Set to None, not empty string
        &ENABLE_RECHECK,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_bigmr.gin_key_limit",
        "Sets the maximum number of bi-gram keys allowed to use for GIN index search.",
        "Zero means no limit.",
        &GIN_KEY_LIMIT,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "pg_bigmr.similarity_limit",
        "Sets the similarity threshold used by the =% operator.",
        "", // TODO: Set to None, not empty string
        &SIMILARITY_LIMIT,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    // Can't be set in postgresql.conf
    GucRegistry::define_string_guc(
        "pg_bigmr.last_update",
        "Shows the last update date of pg_bigm.",
        "",
        &BIGM_LAST_UPDATE,
        GucContext::Internal,
        GucFlags::REPORT | GucFlags::DISALLOW_IN_FILE, // TODO: Add "| GUC_NOT_IN_SAMPLE"
    );

    // TODO: Uncomment after implementation to pgrx is done
    // MarkGUCPrefixReserved("pg_bigmr");
}

pub fn enable_recheck() -> bool {
    ENABLE_RECHECK.get()
}

pub fn gin_key_limit() -> i32 {
    GIN_KEY_LIMIT.get()
}

pub fn similarity_limit() -> f64 {
    SIMILARITY_LIMIT.get()
}
