mod ident;
pub mod client;

use heck::SnakeCase;
use ident::{to_snake, to_upper_camel};
use proc_macro2::{Ident, TokenStream};
use prost_types::FileDescriptorSet;
use itertools::Itertools;
use prost_build::protoc;
use prost::Message;
use std::{ffi::OsStr, fs, path::{Path, PathBuf}, process::Command, str::FromStr};
use quote::quote;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn gen_grpc_server_impl(protos: &[impl AsRef<Path>], out_dir: impl Into<PathBuf>) -> Result<()> {
    let output: PathBuf = out_dir.into();
    for proto in protos {
        let set = gen_file_descriptor(proto)?;
        let file = set.file[0].clone();
        let mod_name = file.package();
        let service = set.file[0].service[0].clone();
        let trait_name = service.name().to_string();
        let service_name = format!("{}Server", trait_name);
        let server_mod = service_name.clone().to_snake_case();
        let method = service.method[0].clone();
        let fn_name = method.name().to_snake_case();
        let input_type = method.input_type();
        let req = get_req_or_ret(mod_name, input_type);
        let output_type = method.output_type();
        let ret = get_req_or_ret(mod_name, output_type);
        let handler_name = "GrpcHandler".to_string();
        let mut buf = String::new();
        generate(trait_name, fn_name, req, mod_name.to_string(), ret, handler_name, service_name, server_mod, &mut buf);
        let file_name = output.join("grpc.rs");
        fs::write(file_name.clone(), buf)?;
        apply_rustfmt(file_name)?;
    }
    Ok(())
}

fn apply_rustfmt(gen_file: impl AsRef<OsStr>) -> Result<()> {
    let mut cmd = Command::new("rustfmt");
    cmd.arg("--edition")
        .arg("2018")
        .arg(gen_file);
    cmd.status()?;
    Ok(())
}

fn gen_file_descriptor(proto_file: impl AsRef<Path>) -> Result<FileDescriptorSet> {
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let mut cmd = Command::new(protoc());
    cmd.arg("-o")
        .arg(&file_descriptor_set_path)
        .arg("-I").arg(".")
        .arg(proto_file.as_ref());
    cmd.status()?;

    let buf = fs::read(file_descriptor_set_path)?;
    tmp.close()?;
    Ok(FileDescriptorSet::decode(&*buf)?)
}

fn get_req_or_ret(package: &str, pb_ident: &str) -> String {
    let mut local_path = package.split('.').peekable();

    let mut ident_path = pb_ident[1..].split('.');
    let ident_type = ident_path.next_back().unwrap();
    let mut ident_path = ident_path.peekable();

    // Skip path elements in common.
    while local_path.peek().is_some() && local_path.peek() == ident_path.peek() {
        local_path.next();
        ident_path.next();
    }

    local_path
        .map(|_| "super".to_string())
        .chain(ident_path.map(to_snake))
        .chain(std::iter::once(to_upper_camel(ident_type)))
        .join("::")
}

fn generate(trait_name: String, fn_name: String, arg: String, mod_name: String, ret: String, handler_name: String, service_name: String, server_mod: String, buf: &mut String) {
    let trait_ident = quote::format_ident!("{}", trait_name);
    let fn_ident = quote::format_ident!("{}", fn_name);
    let req_ident = quote::format_ident!("{}", arg);
    let ret_ident = quote::format_ident!("{}", ret);
    let handler_ident = quote::format_ident!("{}", handler_name);
    let service_ident = quote::format_ident!("{}", service_name);
    let mod_ident = quote::format_ident!("{}", mod_name);
    let server_mod_ident = quote::format_ident!("{}", server_mod);
    let trait_impl = gen_trait_impl(trait_ident, handler_ident.clone(), fn_ident, req_ident, ret_ident.clone(), mod_ident.clone(), server_mod_ident.clone());
    let handler = gen_handler(handler_ident.clone());
    let service_gen = gen_grpc_service(service_ident, handler_ident, mod_ident.clone(), server_mod_ident);
    let proto_mod = gen_tonic_mod(mod_ident.clone());
    let response_msg = gen_response_msg(mod_ident.clone(), ret_ident.clone());
    let grpc_response = gen_grpc_response(mod_ident, ret_ident);
    let code = quote! {
        #proto_mod

        #handler

        #response_msg

        #service_gen

        #trait_impl

        #grpc_response
    };
    let formatted_code = format!("{}", code);
    buf.push_str(&formatted_code);
}

fn gen_tonic_mod(mod_ident: Ident) -> TokenStream {
    let mod_str = format!("\"{}\"", mod_ident);
    let mod_token = TokenStream::from_str(&mod_str).unwrap();
    quote! {
        pub mod #mod_ident {
            tonic::include_proto!(#mod_token);
        }
    }
}

fn gen_trait_impl(trait_ident: Ident, handler_ident: Ident, fn_ident: Ident, req_ident: Ident, ret_ident: Ident, mod_ident: Ident, server_mod_ident: Ident) -> TokenStream {
    let method_impl = gen_trait_methods_impl(fn_ident, req_ident, ret_ident, mod_ident.clone());
    quote! {
        #[tonic::async_trait]
        impl #mod_ident::#server_mod_ident::#trait_ident for #handler_ident {
            #method_impl 
        } 
    }
}

fn gen_trait_methods_impl(fn_ident: Ident, req_ident: Ident, ret_ident: Ident, mod_ident: Ident) -> TokenStream {
    quote! {
        async fn #fn_ident(
            &self,
            request: tonic::Request<#mod_ident::#req_ident>
        ) -> Result<tonic::Response<#mod_ident::#ret_ident>, tonic::Status> {
            println!("Got a request from {:?}", request.remote_addr());
            let (response_tx, response_rx) = async_channel::bounded(100);
            let msg = ResponseMsg {
                addr: request.remote_addr().unwrap(),
                response_tx
            };
            self.tx.send(msg).await.unwrap();
            println!("Preparing response");
            Ok(response_rx.recv().await.unwrap())
        }
    }
}

fn gen_response_msg(mod_ident: Ident, ret_ident: Ident) -> TokenStream {
    quote! {
        #[derive(Clone, Debug)]
        pub struct ResponseMsg {
            pub addr: std::net::SocketAddr,
            pub response_tx: async_channel::Sender<tonic::Response<#mod_ident::#ret_ident>>
        }
    }
}
fn gen_handler(struct_name: Ident) -> TokenStream {
    quote! {
        #[derive(Clone, Debug)]
        pub struct #struct_name {
            tx: async_channel::Sender<ResponseMsg>,
        }
    }
}

fn gen_grpc_service(service_name: Ident, handler_name: Ident, mod_ident: Ident, server_mod_ident: Ident) -> TokenStream {
    quote! {
        pub fn get_grpc_service() -> (#mod_ident::#server_mod_ident::#service_name<#handler_name>, async_channel::Receiver<ResponseMsg>) {
            let (tx, rx) = async_channel::bounded(100);
            let handler = #handler_name {
                tx,
            };
            let service = #mod_ident::#server_mod_ident::#service_name::new(handler);
            (service, rx)
        }
    }
}

fn gen_grpc_response(mod_ident: Ident, ret_ident: Ident) -> TokenStream {
    quote! {
        pub fn get_grpc_response(value: tremor_value::Value) -> #mod_ident::#ret_ident {
            let response: #mod_ident::#ret_ident = tremor_value::structurize(value).unwrap();
            response
        }
    }
}
