//!  Permissive deserializers for tool params.
//!
//!  MCP clients sometimes pass scalars as strings (e.g. `"25"` instead of `25`,
//!  or `"true"` instead of `true`) or arrays as JSON-encoded strings
//!  (e.g. `"[\"a\",\"b\"]"`). These helpers accept either form so tool calls
//!  succeed regardless of how the model serialised the argument.
//!
//!  Use with `#[serde(deserialize_with = "...")]` on the field.

use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use std::fmt;

//  ─── usize ──────────────────────────────────────────────────────────────────

pub fn opt_usize<'de, D>(d: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<usize>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a non-negative integer, integer string, or null")
        }
        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_none<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            d.deserialize_any(V)
        }
        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v as usize))
        }
        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            if v < 0 {
                Err(E::custom(format!("expected non-negative integer, got {}", v)))
            } else {
                Ok(Some(v as usize))
            }
        }
        fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
            if v < 0.0 || v.fract() != 0.0 {
                Err(E::custom(format!("expected integer, got {}", v)))
            } else {
                Ok(Some(v as usize))
            }
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<usize>()
                .map(Some)
                .map_err(|e| E::custom(format!("'{}' is not a valid integer: {}", trimmed, e)))
        }
        fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
    }
    d.deserialize_any(V)
}

//  ─── bool ───────────────────────────────────────────────────────────────────

pub fn opt_bool<'de, D>(d: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<bool>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a boolean, boolean string, or null")
        }
        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_none<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            d.deserialize_any(V)
        }
        fn visit_bool<E: Error>(self, v: bool) -> Result<Self::Value, E> { Ok(Some(v)) }
        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v != 0))
        }
        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v != 0))
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            match v.trim().to_ascii_lowercase().as_str() {
                "" => Ok(None),
                "true" | "1" | "yes" | "y" | "on" => Ok(Some(true)),
                "false" | "0" | "no" | "n" | "off" => Ok(Some(false)),
                other => Err(E::custom(format!("'{}' is not a valid boolean", other))),
            }
        }
        fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
    }
    d.deserialize_any(V)
}

///  Like `opt_bool` but flattens to `bool` (defaulting to false) for fields
///  marked `#[serde(default)]`.
pub fn bool_default<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(opt_bool(d)?.unwrap_or(false))
}

//  ─── Vec<String> ────────────────────────────────────────────────────────────

///  Accept a JSON array, a JSON-encoded array string, a comma-separated string,
///  or a single string (treated as a one-element list).
pub fn vec_string<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an array of strings, JSON array string, or comma-separated string")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(item) = seq.next_element::<StringLike>()? {
                out.push(item.0);
            }
            Ok(out)
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            //  JSON array form: "[...]"
            if trimmed.starts_with('[') {
                return serde_json::from_str::<Vec<StringLike>>(trimmed)
                    .map(|v| v.into_iter().map(|s| s.0).collect())
                    .map_err(|e| E::custom(format!("invalid JSON array: {}", e)));
            }
            //  Comma-separated fallback
            Ok(trimmed
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect())
        }
        fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
    }
    d.deserialize_any(V)
}

pub fn opt_vec_string<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<String>>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an optional array of strings (array, JSON string, or null)")
        }
        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_none<E: Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            vec_string(d).map(Some)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            //  Re-dispatch to vec_string visitor via a tiny shim deserializer.
            //  Simpler: collect inline.
            let mut s = seq;
            let mut out = Vec::with_capacity(s.size_hint().unwrap_or(0));
            while let Some(item) = s.next_element::<StringLike>()? {
                out.push(item.0);
            }
            Ok(Some(out))
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            if trimmed.starts_with('[') {
                return serde_json::from_str::<Vec<StringLike>>(trimmed)
                    .map(|v| Some(v.into_iter().map(|s| s.0).collect()))
                    .map_err(|e| E::custom(format!("invalid JSON array: {}", e)));
            }
            Ok(Some(
                trimmed
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            ))
        }
        fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
    }
    d.deserialize_any(V)
}

//  ─── helper: a String that also accepts non-string scalars ──────────────────

///  Wrapper that deserializes from any scalar (string/number/bool) into a String.
///  Used inside vec_string visitors so an array of mixed scalars still works.
struct StringLike(String);

impl<'de> serde::Deserialize<'de> for StringLike {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = StringLike;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string or scalar")
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StringLike(v.to_string()))
            }
            fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StringLike(v))
            }
            fn visit_bool<E: Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StringLike(v.to_string()))
            }
            fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StringLike(v.to_string()))
            }
            fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StringLike(v.to_string()))
            }
            fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(StringLike(v.to_string()))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TestU {
        #[serde(default, deserialize_with = "opt_usize")]
        x: Option<usize>,
    }

    #[derive(Deserialize)]
    struct TestB {
        #[serde(default, deserialize_with = "bool_default")]
        x: bool,
    }

    #[derive(Deserialize)]
    struct TestOptB {
        #[serde(default, deserialize_with = "opt_bool")]
        x: Option<bool>,
    }

    #[derive(Deserialize)]
    struct TestV {
        #[serde(deserialize_with = "vec_string")]
        x: Vec<String>,
    }

    #[derive(Deserialize)]
    struct TestOptV {
        #[serde(default, deserialize_with = "opt_vec_string")]
        x: Option<Vec<String>>,
    }

    fn parse<T: for<'de> Deserialize<'de>>(s: &str) -> T {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn opt_usize_native_int() {
        let r: TestU = parse(r#"{"x": 25}"#);
        assert_eq!(r.x, Some(25));
    }

    #[test]
    fn opt_usize_string() {
        let r: TestU = parse(r#"{"x": "25"}"#);
        assert_eq!(r.x, Some(25));
    }

    #[test]
    fn opt_usize_null() {
        let r: TestU = parse(r#"{"x": null}"#);
        assert_eq!(r.x, None);
    }

    #[test]
    fn opt_usize_empty_string() {
        let r: TestU = parse(r#"{"x": ""}"#);
        assert_eq!(r.x, None);
    }

    #[test]
    fn opt_usize_missing() {
        let r: TestU = parse(r#"{}"#);
        assert_eq!(r.x, None);
    }

    #[test]
    fn opt_usize_bad_string_errors() {
        let r: Result<TestU, _> = serde_json::from_str(r#"{"x": "abc"}"#);
        assert!(r.is_err());
    }

    #[test]
    fn bool_default_native() {
        let r: TestB = parse(r#"{"x": true}"#);
        assert!(r.x);
    }

    #[test]
    fn bool_default_string_true() {
        let r: TestB = parse(r#"{"x": "true"}"#);
        assert!(r.x);
    }

    #[test]
    fn bool_default_string_yes() {
        let r: TestB = parse(r#"{"x": "yes"}"#);
        assert!(r.x);
    }

    #[test]
    fn bool_default_string_false() {
        let r: TestB = parse(r#"{"x": "false"}"#);
        assert!(!r.x);
    }

    #[test]
    fn bool_default_int_one() {
        let r: TestB = parse(r#"{"x": 1}"#);
        assert!(r.x);
    }

    #[test]
    fn bool_default_missing() {
        let r: TestB = parse(r#"{}"#);
        assert!(!r.x);
    }

    #[test]
    fn opt_bool_string() {
        let r: TestOptB = parse(r#"{"x": "true"}"#);
        assert_eq!(r.x, Some(true));
    }

    #[test]
    fn vec_string_native_array() {
        let r: TestV = parse(r#"{"x": ["a", "b"]}"#);
        assert_eq!(r.x, vec!["a", "b"]);
    }

    #[test]
    fn vec_string_json_string() {
        let r: TestV = parse(r#"{"x": "[\"a\", \"b\"]"}"#);
        assert_eq!(r.x, vec!["a", "b"]);
    }

    #[test]
    fn vec_string_csv() {
        let r: TestV = parse(r#"{"x": "a, b, c"}"#);
        assert_eq!(r.x, vec!["a", "b", "c"]);
    }

    #[test]
    fn vec_string_mixed_scalars() {
        let r: TestV = parse(r#"{"x": [1, "two", true]}"#);
        assert_eq!(r.x, vec!["1", "two", "true"]);
    }

    #[test]
    fn opt_vec_string_native() {
        let r: TestOptV = parse(r#"{"x": ["a"]}"#);
        assert_eq!(r.x, Some(vec!["a".to_string()]));
    }

    #[test]
    fn opt_vec_string_json_string() {
        let r: TestOptV = parse(r#"{"x": "[\"a\"]"}"#);
        assert_eq!(r.x, Some(vec!["a".to_string()]));
    }

    #[test]
    fn opt_vec_string_null() {
        let r: TestOptV = parse(r#"{"x": null}"#);
        assert_eq!(r.x, None);
    }

    #[test]
    fn opt_vec_string_empty_string() {
        let r: TestOptV = parse(r#"{"x": ""}"#);
        assert_eq!(r.x, None);
    }

    #[test]
    fn opt_vec_string_missing() {
        let r: TestOptV = parse(r#"{}"#);
        assert_eq!(r.x, None);
    }
}
