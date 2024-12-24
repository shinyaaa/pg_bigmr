#[derive(Debug)]
pub struct BigramList {
    pub bigrams: Vec<String>,
    pub removed_dups: bool,
}

impl BigramList {
    pub fn from_value(value: &str) -> Self {
        let padded_value = format!(" {} ", value);
        let bigrams = Self::make_bigrams(&padded_value);
        Self::remove_duplicate_bigms(bigrams)
    }

    pub fn from_query(query: &str) -> Self {
        let mut bigrams: Vec<String> = Vec::new();
        let mut query_iter = query.chars();
        while let Some(s) = Self::get_wildcard_part(&mut query_iter) {
            bigrams.extend(Self::make_bigrams(&s));
        }
        Self::remove_duplicate_bigms(bigrams)
    }

    // Adds bigrams from words (already padded).
    fn make_bigrams(padded_str: &str) -> Vec<String> {
        let mut bigrams = Vec::new();
        let mut chars = padded_str.chars().peekable();
        while let Some(c) = chars.next() {
            if chars.peek().is_none() {
                break;
            } else {
                bigrams.push(format!("{}{}", c, chars.peek().unwrap()));
            }
        }
        bigrams
    }

    fn remove_duplicate_bigms(mut bigrams: Vec<String>) -> Self {
        let original_len = bigrams.len();
        bigrams.sort();
        bigrams.dedup();

        if original_len == bigrams.len() {
            Self {
                bigrams,
                removed_dups: false,
            }
        } else {
            Self {
                bigrams,
                removed_dups: true,
            }
        }
    }

    fn get_wildcard_part<I>(query_iter: I) -> Option<String>
    where
        I: Iterator<Item = char>,
    {
        let mut in_leading_wildcard_meta = false;
        let mut in_escape = false;
        let mut res = String::new();
        let mut query_iter_peekable = query_iter.peekable();

        // Find the first word character, remembering whether preceding character
        // was wildcard meta-character.  Note that the in_escape state persists
        // from this loop to the next one, since we may exit at a word character
        // that is in_escape.
        query_iter_peekable.peek()?;

        while let Some(&c) = query_iter_peekable.peek() {
            if in_escape {
                if c != ' ' {
                    break;
                };
                in_escape = false;
                in_leading_wildcard_meta = false;
            } else if c == '\\' {
                in_escape = true;
            } else if c == '%' || c == '_' {
                in_leading_wildcard_meta = true;
            } else if c != ' ' {
                break;
            } else {
                in_leading_wildcard_meta = false;
            };
            query_iter_peekable.next();
        }

        // Handle string end.
        // if query_iter_peekable.peek().is_none() {
        //     return None;
        // }

        // Add left padding spaces if preceding character wasn't wildcard
        // meta-character.
        if !in_leading_wildcard_meta {
            res.push(' ');
        };

        // Copy data into buf until wildcard meta-character, non-word character or
        // string boundary.  Strip escapes during copy.
        while let Some(&c) = query_iter_peekable.peek() {
            if in_escape {
                if c != ' ' {
                    res.push(c);
                } else {
                    // Back up endword to the escape character when stopping at an
                    // escaped char, so that subsequent get_wildcard_part will
                    // restart from the escape character.  We assume here that
                    // escape chars are single-byte.

                    // TODO
                }
                in_escape = false;
            } else if c == '\\' {
                in_escape = true;
            } else if c == '%' || c == '_' {
                in_leading_wildcard_meta = true;
                break;
            } else if c != ' ' {
                res.push(c);
            } else {
                break;
            };
            query_iter_peekable.next();
        }

        // Add right padding spaces if next character isn't wildcard
        // meta-character.
        if !in_leading_wildcard_meta {
            res.push(' ');
        };
        Some(res)
    }
}
