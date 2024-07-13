extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Fields, FieldsNamed};

fn gen_encode_quote_for_struct(my_struct: DataStruct) -> proc_macro2::TokenStream {
    match my_struct.fields {
        Fields::Named(fields) => get_encode_quote(fields),
        Fields::Unnamed(_) => unimplemented!(),
        Fields::Unit => unimplemented!(),
    }
}

fn gen_get_size_quote_for_struct(my_struct: DataStruct) -> proc_macro2::TokenStream {
    match my_struct.fields {
        Fields::Named(fields) => get_size_quote(fields),
        Fields::Unnamed(_) => unimplemented!(),
        Fields::Unit => unimplemented!(),
    }
}

fn gen_encode_quote_for_enum(my_enum: DataEnum) -> proc_macro2::TokenStream {
    let variants = my_enum.variants.iter().map(|v| &v.ident);

    quote! {
        match self {
            #(Self::#variants(inner) => ::franz_core::Encode::encode(inner, dst),)*
        }
    }
}

fn gen_get_size_quote_for_enum(my_enum: DataEnum) -> proc_macro2::TokenStream {
    let variants = my_enum.variants.iter().map(|v| &v.ident);

    quote! {
        match self {
            #(Self::#variants(inner) => ::franz_core::Encode::get_size(inner),)*
        }
    }
}

pub fn derive_proc_macro_impl(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, .. } = parse_macro_input!(input as DeriveInput);

    let encode_str = match data.clone() {
        Data::Struct(my_struct) => gen_encode_quote_for_struct(my_struct),
        Data::Enum(my_enum) => gen_encode_quote_for_enum(my_enum),
        Data::Union(_) => unimplemented!(),
    };

    let get_size_str = match data {
        Data::Struct(my_struct) => gen_get_size_quote_for_struct(my_struct),
        Data::Enum(my_enum) => gen_get_size_quote_for_enum(my_enum),
        Data::Union(_) => unimplemented!(),
    };

    quote! {
        impl ::franz_core::Encode for #ident {
            fn get_size(&self) -> i32 {
                #get_size_str

            }

            fn encode(&self, dst: &mut BytesMut) {
                #encode_str
            }
      }
    }
    .into()
}

fn get_size_quote(fields: FieldsNamed) -> proc_macro2::TokenStream {
    let named_field_idents = fields.named.iter().map(|f| &f.ident);

    let quote = quote! {
        #(::franz_core::Encode::get_size(&self.#named_field_idents))+*
    };

    quote
}

fn get_encode_quote(fields: FieldsNamed) -> proc_macro2::TokenStream {
    let named_field_idents = fields.named.iter().map(|f| &f.ident);

    let quote = quote! {
        #(::franz_core::Encode::encode(&self.#named_field_idents, dst);)*

    };

    quote
}
