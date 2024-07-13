mod decode;
mod encode;
use proc_macro::TokenStream;

#[proc_macro_derive(Decode)]
pub fn derive_macro_decode(input: TokenStream) -> TokenStream {
    decode::derive_proc_macro_impl(input)
}

#[proc_macro_derive(Encode)]
pub fn derive_macro_encode(input: TokenStream) -> TokenStream {
    encode::derive_proc_macro_impl(input)
}
