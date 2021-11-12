use proc_macro2::{Ident, TokenStream};
use quote::quote;
use crate::GenProtoInfo;

pub fn gen_handler(struct_name: Ident) -> TokenStream {
    quote! {
        #[derive(Clone, Debug)]
        pub struct #struct_name {
            pub sink: super::connectors::grpc::GrpcSink,
            pub source: super::connectors::source::ChannelSourceRuntime,
            pub stream_id_gen: super::connectors::StreamIdGen,
            pub context: super::connectors::ConnectorContext
        }
    }
}

pub fn gen_trait_impl(
    trait_ident: Ident,
    grpc_handler_ident: Ident,
    method_ident: Ident,
    request_message_ident: Ident,
    response_message_ident: Ident,
    package_ident: Ident,
    server_mod_ident: Ident,
) -> TokenStream {
    let method_impl = gen_trait_methods_impl(
        method_ident,
        request_message_ident,
        response_message_ident,
        package_ident.clone(),
    );
    quote! {
        #[tonic::async_trait]
        impl #package_ident::#server_mod_ident::#trait_ident for #grpc_handler_ident {
            #method_impl
        }
    }
}

pub fn gen_trait_methods_impl(
    fn_ident: Ident,
    req_ident: Ident,
    ret_ident: Ident,
    mod_ident: Ident,
) -> TokenStream {
    quote! {
        async fn #fn_ident(
            &self,
            request: tonic::Request<#mod_ident::#req_ident>
        ) -> Result<tonic::Response<#mod_ident::#ret_ident>, tonic::Status> {
            let remote_addr = request.remote_addr();
            trace!(
                "[Connector::{}] new connection from {}",
                &self.url.clone(),
                remote_addr.clone()
            );
            let host = remote_addr.and_then(|addr| addr.ip().to_string()).unwrap_or("".to_string());
            let port = remote_addr.and_then(|addr| addr.port());
            let stream_id: u64 = self.stream_id_gen.next_stream_id();

            let origin_uri = tremor_pipeline::EventOriginUri {
                scheme: super::URL_SCHEME.to_string(),
                host: remote_addr.and_then(|addr| addr.ip().to_string()).unwrap_or("".to_string()),
                port: remote_addr.and_then(|addr| addr.port()),
                path: vec![]
            };
            let meta = tremor_value::literal!({
                "remote": {
                    "host": host,
                    "port": port
                }
                "stream_id": stream_id
            });
            let message = request.into_inner();
            let value = tremor_value::Value::from(message);
            let grpc_unary_reader = super::connectors::grpc::unary::GrpcUnaryReader {
                value,
                url: self.context.url.clone(),
                origin_uri,
                meta
            };
            self.source.register_stream_reader(stream_id, &self.context, grpc_unary_reader);
            let (response_tx, response_rx) = async_channel::bounded(100);
            let grpc_unary_writer = super::connectors::grpc::unary::GrpcUnaryWriter {
                tx: response_tx
            };
            self.sink.register_structured_stream_writer(stream_id, meta, &self.context, grpc_unary_writer);
            let value = response_rx.recv().await.unwrap();
            println!("Preparing response");
            let message: #mod_ident::#ret_ident = tremor_value::structurize(value).unwrap();
            Ok(tonic::Response::new(message))
        }
    }
}

pub fn gen_grpc_service(
    service_name: Ident,
    handler_name: Ident,
    mod_ident: Ident,
    server_mod_ident: Ident,
) -> TokenStream {
    quote! {
        pub fn get_grpc_router(
            server: tonic::transport::server::Server<tower_layer::Identity>,
            source: super::connectors::source::ChanelSourceRuntime,
            sink: super::connectors::grpc::GrpcSink,
            ctx: super::connectors::ConnectorContext
        ) -> (#mod_ident::#server_mod_ident::#service_name<#handler_name>, async_channel::Receiver<ResponseMsg>) {
            let stream_id_gen = super::connectors::StreamIdGen::default();
            let handler = #handler_name {
                sink,
                source,
                stream_id_gen,
                ctx
            };
            let service = #mod_ident::#server_mod_ident::#service_name::new(handler);
            server.add_service(service)
        }
    }
}

pub fn generate_grpc_server_impl(proto_info: &GenProtoInfo) -> String {
    let trait_ident = quote::format_ident!("{}", proto_info.trait_name);
    let method_ident = quote::format_ident!("{}", proto_info.method_name);
    let request_message_ident = quote::format_ident!("{}", proto_info.request_message_name);
    let response_message_ident = quote::format_ident!("{}", proto_info.response_message_name);
    let grpc_handler_ident = quote::format_ident!("{}", proto_info.grpc_handler_name);
    let server_ident = quote::format_ident!("{}", proto_info.server_name);
    let package_ident = quote::format_ident!("{}", proto_info.package_name);
    let server_mod_ident = quote::format_ident!("{}", proto_info.server_mod_name);

    let handler = gen_handler(grpc_handler_ident.clone());
    let service_gen = gen_grpc_service(
        server_ident,
        grpc_handler_ident.clone(),
        package_ident.clone(),
        server_mod_ident.clone(),
    );
    let trait_impl = gen_trait_impl(
        trait_ident,
        grpc_handler_ident,
        method_ident,
        request_message_ident,
        response_message_ident.clone(),
        package_ident.clone(),
        server_mod_ident,
    );

    let code = quote! {
        #handler

        #service_gen

        #trait_impl
    };

    format!("{}", code)
}
