pub mod client;
mod ident;
pub mod server;

use heck::SnakeCase;
use ident::{to_snake, to_upper_camel};
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use prost::Message;
use prost_build::protoc;
use prost_types::FileDescriptorSet;
use quote::quote;
use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// type ServiceDescriptorProto = prost_types::ServiceDescriptorProto;
// type MethodDescriptorProto = prost_types::MethodDescriptorProto;
// 
// trait MethodDescriptor {
    // fn method_snake_cased(&self) -> String
// }
// 
// impl crate::MethodDescriptorProto {
    // fn method_snake_cased(&self) -> String {
        // &self.name().to_snake_case()
    // }
// 
    // fn request_message(&self, package: &str) -> String {
        // get_req_or_ret(package, self.input_type()) 
    // }
// }

pub struct GenProtoInfo {
    trait_name: String,
    method_name: String,
    request_message_name: String,
    response_message_name: String,
    package_name: String,
    grpc_handler_name: String,
    server_name: String,
    client_name: String,
    server_mod_name: String,
    client_mod_name: String
}

impl From<FileDescriptorSet> for GenProtoInfo {
    fn from(set: FileDescriptorSet) -> Self {
        let file = set.file[0].clone();
        let service = set.file[0].service[0].clone();
        let method = service.method[0].clone();
        GenProtoInfo {
            trait_name: service.name().to_string(),
            method_name: method.name().to_snake_case(),
            request_message_name: get_req_or_ret(file.package(), method.input_type()),
            response_message_name: get_req_or_ret(file.package(), method.output_type()),
            package_name: file.package().to_string(),
            grpc_handler_name: "GrpcHandler".to_string(),
            server_name: format!("{}Server", service.name()),
            client_name: format!("{}Client", service.name()),
            server_mod_name: format!("{}Server", service.name()).to_snake_case(),
            client_mod_name: format!("{}Client", service.name()).to_snake_case()
        }
    }
}

pub fn generate(protos: &[impl AsRef<Path>], out_dir: impl Into<PathBuf>, server: bool, client: bool) -> Result<()> {
    let output: PathBuf = out_dir.into();
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output.join("grpc.rs"))?;
    for proto in protos {
        let set = gen_file_descriptor(proto)?;
        let proto_info = GenProtoInfo::from(set);
        let mut buf = String::new();
        let package_ident = quote::format_ident!("{}", proto_info.package_name);
        let proto_mod = format!("{}", gen_tonic_mod(package_ident.clone()));
        buf.push_str(&proto_mod);
        if server {
            let server_code = server::generate_grpc_server_impl(&proto_info);
            buf.push_str(&server_code);
        }
        if client {
            let client_code = client::generate_grpc_client_impl(&proto_info);
            buf.push_str(&client_code);
        }
        fs::write(output.join("grpc.rs"), buf)?;
        apply_rustfmt(output.join("grpc.rs")).unwrap();
    }
    Ok(())
}

fn gen_tonic_mod(package_ident: Ident) -> TokenStream {
    let mod_str = format!("\"{}\"", package_ident);
    let mod_token = TokenStream::from_str(&mod_str).unwrap();
    quote! {
        pub mod #package_ident {
            tonic::include_proto!(#mod_token);
        }
    }
}


fn apply_rustfmt(gen_file: impl AsRef<OsStr>) -> Result<()> {
    let mut cmd = Command::new("rustfmt");
    cmd.arg("--edition").arg("2018").arg(gen_file);
    cmd.status()?;
    Ok(())
}

fn gen_file_descriptor(proto_file: impl AsRef<Path>) -> Result<FileDescriptorSet> {
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let mut cmd = Command::new(protoc());
    cmd.arg("-o")
        .arg(&file_descriptor_set_path)
        .arg("-I")
        .arg(".")
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

// fn gen_response_msg(mod_ident: Ident, ret_ident: Ident) -> TokenStream {
    // quote! {
        // #[derive(Clone, Debug)]
        // pub struct ResponseMsg {
            // pub addr: std::net::SocketAddr,
            // pub response_tx: async_channel::Sender<tonic::Response<#mod_ident::#ret_ident>>
        // }
    // }
// }
// 
// fn gen_grpc_response(mod_ident: Ident, ret_ident: Ident) -> TokenStream {
    // quote! {
        // pub fn get_grpc_response(value: tremor_value::Value) -> #mod_ident::#ret_ident {
            // let response: #mod_ident::#ret_ident = tremor_value::structurize(value).unwrap();
            // response
        // }
    // }
// }
