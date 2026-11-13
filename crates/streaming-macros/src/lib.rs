//! The derives of streaming-rs.
//!
//! `#[derive(Operator)]` turns a structure into the configuration of an
//! operator: its fields are the configuration, those marked `#[param]` are the
//! ones the API and the UI may change. `#[derive(Param)]` turns an enumeration
//! without data into a parameter offered as a list of choices.
//!
//! Both are documented where they are re-exported, in `streaming-core`.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, ExprUnary, Fields, Ident, Lit, LitStr, Path, Token, Type, UnOp,
    parse::{Parse, ParseStream},
    parse_macro_input,
    spanned::Spanned,
};

/// See `streaming_core::Operator`.
#[proc_macro_derive(Operator, attributes(operator, param))]
pub fn operator(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_operator(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// See `streaming_core::Param`.
#[proc_macro_derive(Param, attributes(param))]
pub fn param(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_param(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The path of `streaming-core` as the crate being compiled names it.
fn core_path() -> Tokens {
    let named = |name: String| Ident::new(&name, Span::call_site());
    match crate_name("streaming-core") {
        Ok(FoundCrate::Itself) => return quote!(::streaming_core),
        Ok(FoundCrate::Name(name)) => {
            let name = named(name);
            return quote!(::#name);
        }
        Err(_) => {}
    }
    match crate_name("streaming") {
        Ok(FoundCrate::Itself) => quote!(::streaming::core),
        Ok(FoundCrate::Name(name)) => {
            let name = named(name);
            quote!(::#name::core)
        }
        Err(_) => quote!(::streaming_core),
    }
}

/// The first paragraph of a documentation comment, on one line.
fn first_paragraph(attrs: &[Attribute]) -> String {
    let mut lines = Vec::new();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("doc")) {
        let syn::Meta::NameValue(meta) = &attr.meta else {
            continue;
        };
        let Expr::Lit(ExprLit {
            lit: Lit::Str(text), ..
        }) = &meta.value
        else {
            continue;
        };
        let line = text.value().trim().to_owned();
        if line.is_empty() {
            if lines.is_empty() {
                continue;
            }
            break;
        }
        lines.push(line);
    }
    lines.join(" ")
}

/// `rate_per_sec` as the configuration names it: `ratePerSec`.
fn camel_case(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut upper = false;
    for c in field.trim_start_matches("r#").chars() {
        if c == '_' {
            upper = !out.is_empty();
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// `EarliestFirst` as the configuration names it: `earliest-first`.
fn kebab_case(variant: &str) -> String {
    let mut out = String::with_capacity(variant.len() + 4);
    for c in variant.chars() {
        if c.is_uppercase() {
            if !out.is_empty() {
                out.push('-');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// A literal of an attribute: what it is worth in a configuration, and how a
/// message shows it.
struct Constant {
    json: Tokens,
    shown: String,
}

fn constant(core: &Tokens, expr: &Expr) -> syn::Result<Constant> {
    let value = quote!(#core::__private::Value);
    let refused = || syn::Error::new(expr.span(), "expected a number, a text or a boolean");
    let (negative, lit) = match expr {
        Expr::Lit(ExprLit { lit, .. }) => (false, lit),
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(_), expr, ..
        }) => match &**expr {
            Expr::Lit(ExprLit { lit, .. }) => (true, lit),
            _ => return Err(refused()),
        },
        _ => return Err(refused()),
    };
    Ok(match lit {
        Lit::Int(int) if negative => {
            let number: i64 = format!("-{}", int.base10_digits())
                .parse()
                .map_err(|_| syn::Error::new(int.span(), "this number does not fit in 64 bits"))?;
            Constant {
                json: quote!(#value::from(#number)),
                shown: number.to_string(),
            }
        }
        Lit::Int(int) => {
            let number: u64 = int
                .base10_parse()
                .map_err(|_| syn::Error::new(int.span(), "this number does not fit in 64 bits"))?;
            Constant {
                json: quote!(#value::from(#number)),
                shown: number.to_string(),
            }
        }
        Lit::Float(float) => {
            let number: f64 = float.base10_parse()?;
            let number = if negative { -number } else { number };
            Constant {
                json: quote!(#value::from(#number)),
                shown: number.to_string(),
            }
        }
        Lit::Str(text) if !negative => {
            let text = text.value();
            Constant {
                json: quote!(#value::from(#text)),
                shown: text,
            }
        }
        Lit::Bool(flag) if !negative => {
            let flag = flag.value;
            Constant {
                json: quote!(#value::from(#flag)),
                shown: flag.to_string(),
            }
        }
        _ => return Err(refused()),
    })
}

/// `#[operator("acme/thing@1", about = "...", check = path)]`.
struct OperatorArgs {
    op: LitStr,
    about: Option<LitStr>,
    check: Option<Path>,
}

impl Parse for OperatorArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut args = Self {
            op: input.parse()?,
            about: None,
            check: None,
        };
        while input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if name == "about" {
                args.about = Some(input.parse()?);
            } else if name == "check" {
                args.check = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(name.span(), "expected `about` or `check`"));
            }
        }
        Ok(args)
    }
}

/// `#[param]` or `#[param(default = 4, min = 1, max = 64, rename = "name")]`.
#[derive(Default)]
struct ParamArgs {
    default: Option<Expr>,
    min: Option<Expr>,
    max: Option<Expr>,
    rename: Option<LitStr>,
}

impl ParamArgs {
    fn of(attrs: &[Attribute]) -> syn::Result<Option<Self>> {
        let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("param")) else {
            return Ok(None);
        };
        let mut args = Self::default();
        if matches!(attr.meta, syn::Meta::Path(_)) {
            return Ok(Some(args));
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("default") {
                args.default = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("min") {
                args.min = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("max") {
                args.max = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("rename") {
                args.rename = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `default`, `min`, `max` or `rename`"));
            }
            Ok(())
        })?;
        Ok(Some(args))
    }
}

/// Whether a type is written `Option<...>`.
fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none()
        && path.path.segments.last().is_some_and(|segment| segment.ident == "Option"))
}

fn optional(tokens: Option<Tokens>) -> Tokens {
    match tokens {
        Some(tokens) => quote!(::std::option::Option::Some(#tokens)),
        None => quote!(::std::option::Option::None),
    }
}

fn derive_operator(input: &DeriveInput) -> syn::Result<Tokens> {
    let core = core_path();
    let private = quote!(#core::__private);
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "an operator is a concrete structure: its type names it in a graph",
        ));
    }
    let args: OperatorArgs = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("operator"))
        .ok_or_else(|| {
            syn::Error::new(
                name.span(),
                "an operator needs its reference: `#[operator(\"acme/thing@1\")]`",
            )
        })?
        .parse_args()?;
    let op = &args.op;
    let about = args
        .about
        .as_ref()
        .map_or_else(|| first_paragraph(&input.attrs), LitStr::value);
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(name.span(), "an operator is a structure"));
    };
    let fields = match &data.fields {
        Fields::Named(fields) => fields.named.iter().collect::<Vec<_>>(),
        Fields::Unit => Vec::new(),
        Fields::Unnamed(fields) => {
            return Err(syn::Error::new(
                fields.span(),
                "the fields of an operator are named: they are its configuration",
            ));
        }
    };
    let unit = matches!(data.fields, Fields::Unit);

    let mut specs = Vec::new();
    let mut checks = Vec::new();
    let mut puts = Vec::new();
    let mut gets = Vec::new();
    let mut names = Vec::new();
    let mut secrets = Vec::new();
    for field in fields {
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        let args = ParamArgs::of(&field.attrs)?;
        let json_name = args
            .as_ref()
            .and_then(|args| args.rename.as_ref().map(LitStr::value))
            .unwrap_or_else(|| camel_case(&ident.to_string()));
        names.push(json_name.clone());
        let Some(args) = args else {
            puts.push(quote! {
                #private::put_field(&mut map, #json_name, &self.#ident)
                    .map_err(<S::Error as #private::serde::ser::Error>::custom)?;
            });
            gets.push(quote!(#ident: #private::field(map, #json_name)?));
            continue;
        };
        let default = args.default.as_ref().map(|expr| constant(&core, expr)).transpose()?;
        let default_json = optional(default.map(|constant| constant.json));
        let description = first_paragraph(&field.attrs);
        let bound = |expr: &Option<Expr>| expr.as_ref().map(|expr| constant(&core, expr)).transpose();
        let (min, max) = (bound(&args.min)?, bound(&args.max)?);
        let said = |bound: &Option<Constant>| optional(bound.as_ref().map(|constant| constant.json.clone()));
        let (min_json, max_json) = (said(&min), said(&max));
        specs.push(quote!(#private::spec::<#ty>(#json_name, #description, #default_json, #min_json, #max_json)));
        puts.push(quote!(#private::put_param(&mut map, #json_name, &self.#ident);));
        secrets.push(quote!(#core::Param::secret(&self.#ident)));
        gets.push(quote!(#ident: #private::param(map, #json_name, #default_json)?));
        if min.is_some() || max.is_some() {
            let checked = |bound: Option<Constant>| {
                optional(bound.map(|constant| {
                    let (json, shown) = (constant.json, constant.shown);
                    quote!((#json, #shown))
                }))
            };
            let (min, max) = (checked(min), checked(max));
            checks.push(if is_option(ty) {
                quote! {
                    if let ::std::option::Option::Some(value) = &self.#ident {
                        #private::range(#json_name, value, #min, #max)?;
                    }
                }
            } else {
                quote!(#private::range(#json_name, &self.#ident, #min, #max)?;)
            });
        }
    }
    let secret_count = secrets.len();
    let user_check = args.check.as_ref().map(|path| quote!(#path(self)?;));

    let serialize = if unit {
        quote!(#private::serde::Serializer::serialize_unit(serializer))
    } else {
        quote! {
            let mut map = #private::Map::new();
            #(#puts)*
            #private::serde::Serialize::serialize(&#private::Value::Object(map), serializer)
        }
    };
    let deserialize = if unit {
        quote! {
            <#private::serde::de::IgnoredAny as #private::serde::Deserialize>::deserialize(deserializer)?;
            ::std::result::Result::Ok(#name)
        }
    } else {
        quote! {
            let value = <#private::Value as #private::serde::Deserialize>::deserialize(deserializer)?;
            let read = |value: &#private::Value| -> ::std::result::Result<#name, ::std::string::String> {
                let map = #private::object(value, &[#(#names),*])?;
                let map = &*map;
                ::std::result::Result::Ok(#name { #(#gets),* })
            };
            read(&value).map_err(<D::Error as #private::serde::de::Error>::custom)
        }
    };

    Ok(quote! {
        const _: () = {
            impl #core::Operator for #name {
                const OP: &'static str = #op;
                const ABOUT: &'static str = #about;

                fn params() -> ::std::vec::Vec<#core::ParamSpec> {
                    ::std::vec![#(#specs),*]
                }

                fn check(&self) -> ::std::result::Result<(), #core::OpError> {
                    #(#checks)*
                    #user_check
                    ::std::result::Result::Ok(())
                }

                fn secrets(&self) -> ::std::vec::Vec<&#core::Secret> {
                    let secrets: [::std::option::Option<&#core::Secret>; #secret_count] = [#(#secrets),*];
                    secrets.into_iter().flatten().collect()
                }
            }

            impl #private::serde::Serialize for #name {
                fn serialize<S: #private::serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> ::std::result::Result<S::Ok, S::Error> {
                    #serialize
                }
            }

            impl<'de> #private::serde::Deserialize<'de> for #name {
                fn deserialize<D: #private::serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> ::std::result::Result<Self, D::Error> {
                    #deserialize
                }
            }
        };
    })
}

fn derive_param(input: &DeriveInput) -> syn::Result<Tokens> {
    let core = core_path();
    let private = quote!(#core::__private);
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(
            name.span(),
            "a parameter offered as a choice is an enumeration",
        ));
    };
    let mut variants = Vec::new();
    let mut texts = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new(variant.span(), "a choice carries no data"));
        }
        let text = ParamArgs::of(&variant.attrs)?
            .and_then(|args| args.rename.map(|name| name.value()))
            .unwrap_or_else(|| kebab_case(&variant.ident.to_string()));
        variants.push(variant.ident.to_token_stream());
        texts.push(text);
    }
    Ok(quote! {
        const _: () = {
            impl #core::Param for #name {
                const KIND: &'static str = "text";

                fn choices() -> ::std::vec::Vec<::std::string::String> {
                    ::std::vec![#(::std::string::String::from(#texts)),*]
                }

                fn to_json(&self) -> #private::Value {
                    #private::Value::from(match self {
                        #(Self::#variants => #texts,)*
                    })
                }

                fn from_json(value: &#private::Value) -> ::std::result::Result<Self, ::std::string::String> {
                    match value.as_str() {
                        #(::std::option::Option::Some(#texts) => ::std::result::Result::Ok(Self::#variants),)*
                        _ => ::std::result::Result::Err(#private::one_of(&[#(#texts),*])),
                    }
                }
            }
        };
    })
}
