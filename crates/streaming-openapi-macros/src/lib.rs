//! The `#[route]` attribute of `streaming-openapi`. Use it through
//! `streaming_openapi::route`, which documents it.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    FnArg, GenericArgument, Ident, ItemFn, LitStr, PathArguments, ReturnType, Token, Type,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

/// Register an axum handler and describe it in the OpenAPI document. See
/// `streaming_openapi::route`.
#[proc_macro_attribute]
pub fn route(attr: TokenStream, item: TokenStream) -> TokenStream {
    match expand(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// HTTP methods, as written in the attribute and as axum names their routers.
const METHODS: [(&str, &str); 8] = [
    ("GET", "get"),
    ("POST", "post"),
    ("PUT", "put"),
    ("DELETE", "delete"),
    ("PATCH", "patch"),
    ("HEAD", "head"),
    ("OPTIONS", "options"),
    ("TRACE", "trace"),
];

/// `GET, "/path", tag = "name"`.
struct RouteArgs {
    /// Lower-case method, the name of axum's method router function.
    method: String,
    path: LitStr,
    tag: Option<LitStr>,
}

impl Parse for RouteArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let method: Ident = input.parse().map_err(|e| {
            syn::Error::new(
                e.span(),
                "expected `#[route(METHOD, \"/path\")]`, for instance `#[route(GET, \"/api/v1/cluster\")]`",
            )
        })?;
        let name = method.to_string();
        let Some((_, router)) = METHODS.iter().find(|(written, _)| *written == name) else {
            let known: Vec<&str> = METHODS.iter().map(|(written, _)| *written).collect();
            return Err(syn::Error::new(
                method.span(),
                format!("unknown HTTP method `{name}`: expected one of {}", known.join(", ")),
            ));
        };
        input
            .parse::<Token![,]>()
            .map_err(|e| syn::Error::new(e.span(), "expected `,` then the path, as in `GET, \"/path\"`"))?;
        let path: LitStr = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "expected the path as a string literal, as in `\"/path\"`"))?;
        let mut tag = None;
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            match key.to_string().as_str() {
                "tag" if tag.is_none() => tag = Some(value),
                "tag" => return Err(syn::Error::new(key.span(), "`tag` is given twice")),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown option `{other}`: the only option is `tag = \"...\"`"),
                    ));
                }
            }
        }
        Ok(Self {
            method: (*router).to_owned(),
            path,
            tag,
        })
    }
}

/// The names of the `{param}` segments of an axum path, in order.
fn path_params(path: &str) -> Result<Vec<String>, String> {
    if !path.starts_with('/') {
        return Err(format!("the path `{path}` must start with `/`"));
    }
    let mut params = Vec::new();
    let mut rest = path;
    while let Some(open) = rest.find(['{', '}']) {
        if rest.as_bytes()[open] == b'}' {
            return Err(format!("the path `{path}` closes a `}}` it did not open"));
        }
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| format!("the path `{path}` opens a `{{` it does not close"))?;
        let name = &after[..close];
        if name.starts_with('*') {
            return Err(format!(
                "the path `{path}` has a wildcard `{{{name}}}`, which OpenAPI cannot describe"
            ));
        }
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "the path `{path}` has a parameter `{{{name}}}`: a name is letters, digits and `_`"
            ));
        }
        if params.iter().any(|known| known == name) {
            return Err(format!("the path `{path}` names `{{{name}}}` twice"));
        }
        params.push(name.to_owned());
        rest = &after[close + 1..];
    }
    Ok(params)
}

/// The last segment of a type path and its generic arguments, if the type is
/// a path.
fn last_segment(ty: &Type) -> Option<(&Ident, Vec<&Type>)> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    let args = match &segment.arguments {
        PathArguments::AngleBracketed(args) => args
            .args
            .iter()
            .filter_map(|arg| match arg {
                GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Some((&segment.ident, args))
}

/// The `T` of an extractor written `Name<T>`.
fn extractor_arg<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    match last_segment(ty) {
        Some((ident, args)) if ident == name && args.len() == 1 => Some(args[0]),
        _ => None,
    }
}

/// Check the `{param}`s of the path against the `Path` extractor.
fn check_path_extractor(path: &LitStr, params: &[String], inputs: &[&Type]) -> syn::Result<()> {
    let extractor = inputs
        .iter()
        .find_map(|ty| extractor_arg(ty, "Path").map(|arg| (ty, arg)));
    match (extractor, params.len()) {
        (None, 0) => Ok(()),
        (None, _) => Err(syn::Error::new(
            path.span(),
            format!(
                "the path names {} but no `Path` extractor reads it",
                params
                    .iter()
                    .map(|p| format!("`{{{p}}}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
        (Some((ty, _)), 0) => Err(syn::Error::new(
            ty.span(),
            "this `Path` extractor reads nothing: the path has no `{param}`",
        )),
        (Some((ty, Type::Tuple(tuple))), expected) if tuple.elems.len() != expected => Err(syn::Error::new(
            ty.span(),
            format!(
                "this `Path` extractor reads {} value(s) but the path has {expected} parameter(s): {}",
                tuple.elems.len(),
                params
                    .iter()
                    .map(|p| format!("`{{{p}}}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
        (Some(_), _) => Ok(()),
    }
}

/// `put_graph` -> `putGraph`: OpenAPI's usual spelling of operation ids.
fn lower_camel_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for c in name.trim_start_matches('_').chars() {
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

/// The text of the `///` comments of the handler.
fn doc_text(function: &ItemFn) -> String {
    let mut lines = Vec::new();
    for attr in &function.attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let syn::Meta::NameValue(meta) = &attr.meta
            && let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(text),
                ..
            }) = &meta.value
        {
            let text = text.value();
            lines.push(text.strip_prefix(' ').unwrap_or(&text).to_owned());
        }
    }
    lines.join("\n")
}

fn expand(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let args: RouteArgs = syn::parse2(attr)?;
    let function: ItemFn = syn::parse2(item)?;
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            function.sig.fn_token.span(),
            "`#[route]` goes on an `async fn` handler",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            function.sig.generics.span(),
            "`#[route]` cannot describe a generic handler: its schemas depend on the type parameters",
        ));
    }
    let params = path_params(&args.path.value()).map_err(|message| syn::Error::new(args.path.span(), message))?;

    let mut inputs = Vec::new();
    for input in &function.sig.inputs {
        match input {
            FnArg::Typed(typed) => inputs.push(&*typed.ty),
            FnArg::Receiver(receiver) => {
                return Err(syn::Error::new(
                    receiver.span(),
                    "`#[route]` goes on a free function, not a method",
                ));
            }
        }
    }
    check_path_extractor(&args.path, &params, &inputs)?;

    let output: Type = match &function.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => {
            if let Type::ImplTrait(_) = &**ty {
                return Err(syn::Error::new(
                    ty.span(),
                    "`#[route]` cannot describe an `impl Trait` return type: name a type that implements `OperationOutput`",
                ));
            }
            (**ty).clone()
        }
    };

    let krate = quote!(::streaming_openapi);
    let name = &function.sig.ident;
    let vis = &function.vis;
    let method = format_ident!("{}", args.method);
    let path = &args.path;
    let operation_id = LitStr::new(&lower_camel_case(&name.to_string()), Span::call_site());
    let docs = LitStr::new(&doc_text(&function), Span::call_site());
    let tag = args.tag.iter();
    let state = inputs.iter().find_map(|ty| extractor_arg(ty, "State"));
    let signature = match state {
        Some(state) => quote!(pub fn route() -> #krate::Route<#state>),
        None => {
            quote!(pub fn route<S>() -> #krate::Route<S> where S: ::core::clone::Clone + ::core::marker::Send + ::core::marker::Sync + 'static)
        }
    };
    let module_doc = format!("Route of [`{name}`](fn@{name}), generated by `#[route]`.");

    Ok(quote! {
        #function

        #[doc = #module_doc]
        #[doc(hidden)]
        #vis mod #name {
            #[allow(unused_imports)]
            use super::*;

            /// The handler, its method, its path and its OpenAPI description.
            #signature {
                fn describe(operation: &mut #krate::OperationContext<'_>) {
                    operation.set_operation_id(#operation_id);
                    operation.set_docs(#docs);
                    #( operation.add_tag(#tag); )*
                    #( <#inputs as #krate::OperationInput>::describe(operation); )*
                    <#output as #krate::OperationOutput>::describe(operation);
                }
                #krate::Route::new(
                    #krate::Method::from_axum_name(stringify!(#method)),
                    #path,
                    #krate::__private::axum::routing::#method(super::#name),
                    describe,
                )
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(attr: TokenStream2, item: TokenStream2) -> String {
        expand(attr, item).expect_err("expected a compile error").to_string()
    }

    #[test]
    fn spells_operation_ids_in_lower_camel_case() {
        assert_eq!(lower_camel_case("put_graph"), "putGraph");
        assert_eq!(lower_camel_case("cluster"), "cluster");
        assert_eq!(lower_camel_case("get_node__config_"), "getNodeConfig");
        assert_eq!(lower_camel_case("_private_route"), "privateRoute");
    }

    #[test]
    fn reads_parameters_in_order() {
        assert_eq!(
            path_params("/t/{tenant}/g/{graph}/x").unwrap(),
            vec!["tenant".to_owned(), "graph".to_owned()]
        );
        assert_eq!(path_params("/healthz").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn rejects_malformed_paths() {
        assert!(path_params("api").unwrap_err().contains("must start with `/`"));
        assert!(path_params("/a/{b").unwrap_err().contains("does not close"));
        assert!(path_params("/a/b}").unwrap_err().contains("did not open"));
        assert!(path_params("/a/{*rest}").unwrap_err().contains("wildcard"));
        assert!(path_params("/a/{}").unwrap_err().contains("a name is"));
        assert!(path_params("/a/{b-c}").unwrap_err().contains("a name is"));
        assert!(path_params("/{a}/{a}").unwrap_err().contains("twice"));
    }

    #[test]
    fn expands_a_handler() {
        let tokens = expand(
            quote!(GET, "/t/{tenant}", tag = "tenants"),
            quote! {
                /// Summary.
                ///
                /// Details.
                async fn tenant(State(state): State<AppState>, Path(tenant): Path<String>) -> Json<View> { todo!() }
            },
        )
        .unwrap()
        .to_string();
        assert!(tokens.contains("mod tenant"), "{tokens}");
        assert!(tokens.contains("Route < AppState >"), "{tokens}");
        assert!(tokens.contains("routing :: get (super :: tenant)"), "{tokens}");
        assert!(tokens.contains("\"Summary.\\n\\nDetails.\""), "{tokens}");
        assert!(tokens.contains("add_tag (\"tenants\")"), "{tokens}");
        assert!(
            tokens.contains("< Path < String > as :: streaming_openapi :: OperationInput >"),
            "{tokens}"
        );
        assert!(
            tokens.contains("< Json < View > as :: streaming_openapi :: OperationOutput >"),
            "{tokens}"
        );
    }

    #[test]
    fn a_handler_without_state_is_generic() {
        let tokens = expand(
            quote!(POST, "/ping"),
            quote!(
                async fn ping() {}
            ),
        )
        .unwrap()
        .to_string();
        assert!(
            tokens.contains("fn route < S > () -> :: streaming_openapi :: Route < S >"),
            "{tokens}"
        );
        assert!(tokens.contains("routing :: post (super :: ping)"), "{tokens}");
        assert!(
            tokens.contains("< () as :: streaming_openapi :: OperationOutput >"),
            "{tokens}"
        );
    }

    #[test]
    fn explains_its_errors() {
        let item = quote!(
            async fn h() {}
        );
        assert!(error(quote!(FETCH, "/"), item.clone()).contains("unknown HTTP method `FETCH`"));
        assert!(error(quote!(get, "/"), item.clone()).contains("unknown HTTP method `get`"));
        assert!(error(quote!("/"), item.clone()).contains("expected `#[route(METHOD"));
        assert!(error(quote!(GET "/"), item.clone()).contains("expected `,`"));
        assert!(error(quote!(GET, x), item.clone()).contains("string literal"));
        assert!(error(quote!(GET, "/", summary = "x"), item.clone()).contains("unknown option `summary`"));
        assert!(error(quote!(GET, "/", tag = "a", tag = "b"), item.clone()).contains("twice"));
        assert!(error(quote!(GET, "nope"), item.clone()).contains("must start with `/`"));
        assert!(
            error(
                quote!(GET, "/"),
                quote!(
                    fn h() {}
                )
            )
            .contains("async fn")
        );
        assert!(
            error(
                quote!(GET, "/"),
                quote!(
                    async fn h<T>() {}
                )
            )
            .contains("generic")
        );
        assert!(
            error(
                quote!(GET, "/"),
                quote!(
                    async fn h() -> impl IntoResponse {}
                )
            )
            .contains("impl Trait")
        );
        assert!(error(quote!(GET, "/{id}"), item.clone()).contains("no `Path` extractor reads it"));
        assert!(
            error(
                quote!(GET, "/"),
                quote!(
                    async fn h(Path(a): Path<String>) {}
                )
            )
            .contains("reads nothing")
        );
        assert!(
            error(
                quote!(GET, "/{a}/{b}"),
                quote!(
                    async fn h(Path(a): Path<(String,)>) {}
                )
            )
            .contains("reads 1 value(s) but the path has 2 parameter(s): `{a}`, `{b}`")
        );
        assert!(
            expand(
                quote!(GET, "/{a}/{b}"),
                quote!(
                    async fn h(Path(p): Path<(String, String)>) {}
                )
            )
            .is_ok()
        );
        assert!(
            expand(
                quote!(GET, "/{a}/{b}", tag = "x",),
                quote!(
                    async fn h(Path(p): Path<Params>) {}
                )
            )
            .is_ok()
        );
    }
}
