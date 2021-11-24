pub mod client;
mod ident;

use heck::SnakeCase;
use ident::{to_snake, to_upper_camel};
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use prost::Message;
use prost_build::protoc;
use prost_types::{FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto};
use quote::quote;
use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

trait ServiceProtoInfo {
    fn client_ident(&self) -> Ident;
    fn server_ident(&self) -> Ident;
    fn server_mod_ident(&self) -> Ident;
    fn client_mod_ident(&self) -> Ident;
    fn trait_ident(&self) -> Ident;
}

trait MethodProtoInfo {
    fn name_ident(&self) -> Ident;
    fn request_message_ident(&self, package: &str) -> Ident;
    fn response_message_ident(&self, package: &str) -> Ident;
}

trait FileProtoInfo {
    fn package_ident(&self) -> Ident;
}

impl FileProtoInfo for FileDescriptorProto {
    fn package_ident(&self) -> Ident {
        quote::format_ident!("{}", self.package())
    }
}

impl ServiceProtoInfo for ServiceDescriptorProto {
    fn client_mod_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Client", self.name()).to_snake_case())
    }
    
    fn server_mod_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Server", self.name()).to_snake_case())
    }

    fn client_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Client", self.name()))
    }

    fn server_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Server", self.name()))
    }
    
    fn trait_ident(&self) -> Ident {
        quote::format_ident!("{}", self.name())
    }
}

impl MethodProtoInfo for MethodDescriptorProto {
    fn name_ident(&self) -> Ident {
        quote::format_ident!("{}", self.name().to_snake_case())
    }

    fn request_message_ident(&self, package: &str) -> Ident {
        quote::format_ident!("{}", get_message_type(package, self.input_type()))
    }

    fn response_message_ident(&self, package: &str) -> Ident {
        quote::format_ident!("{}", get_message_type(package, self.output_type()))
    }
}


pub fn generate(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>], out_dir: impl Into<PathBuf>, _server: bool, client: bool) -> Result<()> {
    let descriptor_set = gen_file_descriptor(protos, includes)?;
    if client {
        let output_file = out_dir.into().join("grpc_client.rs");
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_file.clone())?;
        let mut buf = String::new();
        let tonic_modules = format!("{}", gen_tonic_mod(descriptor_set.file.clone()));
        buf.push_str(&tonic_modules);
        let grpc_client_code = client::generate_grpc_client_impl(descriptor_set.file.clone()); 
        buf.push_str(&grpc_client_code);
        fs::write(output_file.clone(), buf)?;
        apply_rustfmt(output_file, "2018")?;
    }
    Ok(())
}

fn gen_tonic_mod(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut modules = vec![];
    for file in files {
        //TODO: Use Literal::String here instead.
        let package_ident = quote::format_ident!("{}", file.package());
        let mod_str = format!("\"{}\"", package_ident);
        let mod_token = TokenStream::from_str(&mod_str).unwrap();
        modules.push(quote! {
            pub mod #package_ident {
                tonic::include_proto!(#mod_token);
            }
        });
    }
    let mut code = TokenStream::new();
    code.extend(modules);
    code
}


fn apply_rustfmt(file: impl AsRef<OsStr>, edition: &str) -> Result<()> {
    let mut cmd = Command::new("rustfmt");
    cmd.arg("--edition").arg(edition).arg(file);
    cmd.status()?;
    Ok(())
}

fn gen_file_descriptor(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> Result<FileDescriptorSet> {
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let mut cmd = Command::new(protoc());
    cmd.arg("--include_imports")
        .arg("--include_source_info")
        .arg("-o")
        .arg(&file_descriptor_set_path);
    for include in includes {
        cmd.arg("-I").arg(include.as_ref());
    }

    for proto in protos {
        cmd.arg(proto.as_ref());
    }

    cmd.status()?;

    let buf = fs::read(file_descriptor_set_path)?;
    tmp.close()?;
    Ok(FileDescriptorSet::decode(&*buf)?)
}

fn get_message_type(package: &str, pb_ident: &str) -> String {
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
