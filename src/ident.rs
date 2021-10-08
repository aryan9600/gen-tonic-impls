use heck::{CamelCase, SnakeCase};

/// Converts a `camelCase` or `SCREAMING_SNAKE_CASE` identifier to a `lower_snake` case Rust field
/// identifier.
pub fn to_snake(s: &str) -> String {
    let mut ident = s.to_snake_case();

    // Use a raw identifier if the identifier matches a Rust keyword:
    // https://doc.rust-lang.org/reference/keywords.html.
    match ident.as_str() {
        // 2015 strict keywords.
        | "as" | "break" | "const" | "continue" | "else" | "enum" | "false"
        | "fn" | "for" | "if" | "impl" | "in" | "let" | "loop" | "match" | "mod" | "move" | "mut"
        | "pub" | "ref" | "return" | "static" | "struct" | "trait" | "true"
        | "type" | "unsafe" | "use" | "where" | "while"
        // 2018 strict keywords.
        | "dyn"
        // 2015 reserved keywords.
        | "abstract" | "become" | "box" | "do" | "final" | "macro" | "override" | "priv" | "typeof"
        | "unsized" | "virtual" | "yield"
        // 2018 reserved keywords.
        | "async" | "await" | "try" => ident.insert_str(0, "r#"),
        // the following keywords are not supported as raw identifiers and are therefore suffixed with an underscore.
        "self" | "super" | "extern" | "crate" => ident += "_",
        _ => (),
    }
    ident
}

/// Converts a `snake_case` identifier to an `UpperCamel` case Rust type identifier.
pub fn to_upper_camel(s: &str) -> String {
    let mut ident = s.to_camel_case();

    // Suffix an underscore for the `Self` Rust keyword as it is not allowed as raw identifier.
    if ident == "Self" {
        ident += "_";
    }
    ident
}
