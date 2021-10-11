use std::path::{Path, PathBuf};
use heck::SnakeCase;
use quote::quote;

use proc_macro2::{Ident, TokenStream};
use std::fs;

use crate::Result;

pub fn gen_grpc_client_impl(protos: &[impl AsRef<Path>], out_dir: impl Into<PathBuf>) -> Result<()>{
    let output: PathBuf = out_dir.into();
    for proto in protos {
        let set = crate::gen_file_descriptor(proto)?;
        let file = set.file[0].clone();
        let mod_name = file.package();
        let service = set.file[0].service[0].clone();
        let tonic_client_name = format!("{}Client", service.name());
        let client_mod = tonic_client_name.clone().to_snake_case();
        let method = service.method[0].clone();
        let method_name = method.name().to_snake_case();
        let input_type = method.input_type();
        let req = crate::get_req_or_ret(mod_name, input_type);
        let output_type = method.output_type();
        let ret = crate::get_req_or_ret(mod_name, output_type);
        let mut buf = String::new();
        generate_client(tonic_client_name, method_name, client_mod, req, ret, mod_name.to_string(), &mut buf);
        let file_name = output.join("grpc.rs");
        fs::write(file_name.clone(), buf)?;
        crate::apply_rustfmt(file_name)?;
    }
    Ok(())     
}

fn generate_client(tonic_client_name: String, method: String, client_mod: String, req_body: String, resp_body: String, mod_name: String, buf: &mut String) {
    let tonic_client_ident = quote::format_ident!("{}", tonic_client_name);
    let method_ident = quote::format_ident!("{}", method);
    let req_body_ident = quote::format_ident!("{}", req_body);
    let resp_body_ident = quote::format_ident!("{}", resp_body);
    let mod_ident = quote::format_ident!("{}", mod_name);
    let client_mod_ident = quote::format_ident!("{}", client_mod);
    let proto_mod = crate::gen_tonic_mod(mod_ident.clone());
    let grpc_client = gen_grpc_client(tonic_client_ident, method_ident, req_body_ident, resp_body_ident, mod_ident, client_mod_ident);
    let code = quote! {
        #proto_mod

        #grpc_client
    };

    let formatted_code = format!("{}", code);
    buf.push_str(&formatted_code);
}

fn gen_grpc_client(tonic_client_ident: Ident, method_ident: Ident, req_body_ident: Ident, resp_body_ident: Ident, mod_ident: Ident, client_mod_ident: Ident) -> TokenStream {
    quote! {
        #[derive(Debug, Clone)]
        pub struct GrpcClient {
            client: #mod_ident::#client_mod_ident::#tonic_client_ident<tonic::transport::channel::Channel>,
        }

        impl GrpcClient {
            pub async fn connect(addr: String) -> Self {
                let client = #mod_ident::#client_mod_ident::#tonic_client_ident::connect(addr).await.unwrap();
                GrpcClient{ client }
            }

            pub async fn send(&mut self, value: tremor_value::Value<'_>) -> tonic::Response<#mod_ident::#resp_body_ident> {
                let body: #mod_ident::#req_body_ident = tremor_value::structurize(value).unwrap();
                let request = tonic::Request::new(body);
                let resp: tonic::Response<#mod_ident::#resp_body_ident> = self.client.#method_ident(request).await.unwrap();
                resp
            }
        }
    }
}
