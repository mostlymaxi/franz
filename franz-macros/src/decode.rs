extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, FieldsNamed};

fn gen_decode_str_for_struct(my_struct: DataStruct) -> proc_macro2::TokenStream {
    match my_struct.fields {
        Fields::Named(fields) => handle_named_fields(fields),
        Fields::Unnamed(_) => unimplemented!(),
        Fields::Unit => unimplemented!(),
    }
}

pub fn derive_proc_macro_impl(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, .. } = parse_macro_input!(input as DeriveInput);

    let decode_str = match data {
        Data::Struct(my_struct) => gen_decode_str_for_struct(my_struct),
        Data::Enum(_) => unimplemented!(),
        Data::Union(_) => unimplemented!(),
    };

    quote! {
      impl ::franz_core::Decode for #ident {
          fn decode(src: &mut BytesMut) -> #ident {
                #decode_str
          }
      }
    }
    .into()
}

fn handle_named_fields(fields: FieldsNamed) -> proc_macro2::TokenStream {
    let named_field_idents = fields.named.iter().map(|f| &f.ident);
    let named_field_idents_clone = named_field_idents.clone();
    let named_field_tys = fields.named.iter().map(|f| &f.ty);

    let quote = quote! {
        // let _ = <i32 as franz_core::Decode>::decode(src);
        #(let #named_field_idents = <#named_field_tys as franz_core::Decode>::decode(src);)*


        Self {
            #(#named_field_idents_clone,)*
        }
    };

    eprintln!("{quote}");
    quote
}
