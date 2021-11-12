use quote::quote;

use proc_macro2::{Ident, TokenStream};

use crate::GenProtoInfo;
use std::str::FromStr;

pub fn generate_grpc_client_impl(proto_info: &GenProtoInfo) -> String {
    let client_ident = quote::format_ident!("{}", proto_info.client_name);
    let method_ident = quote::format_ident!("{}", proto_info.method_name);
    let request_message_ident = quote::format_ident!("{}", proto_info.request_message_name);
    let response_message_ident = quote::format_ident!("{}", proto_info.response_message_name);
    let pacakge_ident = quote::format_ident!("{}", proto_info.package_name);
    let client_mod_ident = quote::format_ident!("{}", proto_info.client_mod_name);
    let grpc_client = generate_unary_client(
        client_ident,
        method_ident,
        request_message_ident,
        response_message_ident,
        pacakge_ident,
        client_mod_ident,
    );
    let code = quote! {
        #grpc_client
    };

    let formatted_code = format!("{}", code);
    formatted_code
}

fn generate_unary_client(
    client_ident: Ident,
    method_ident: Ident,
    request_message_ident: Ident,
    response_message_ident: Ident,
    package_ident: Ident,
    client_mod_ident: Ident,
) -> TokenStream {
    let key_str = format!("\"{}\"", package_ident);
    let key_token = TokenStream::from_str(&key_str).unwrap();
    quote! {
        use value_trait::ValueAccess;
        #[derive(Debug, Clone)]
        pub struct GrpcClient {
            inner: hashbrown::HashMap<String, Box<dyn Send>>,
        }

        impl GrpcClient {
            pub async fn connect(addr: String) -> Self {
                let client = #package_ident::#client_mod_ident::#client_ident::connect(addr).await.unwrap();
                let inner = hashbrown::HashMap::new();
                inner.insert(String::from(#key_token), Box::new(client));
                GrpcClient{ inner }
            }

            pub async fn send_request(&mut self, value: tremor_value::Value<'_>, meta: tremor_value::Value<'_>) -> tremor_script::EventPayload {
                println!("args: {:?} {:?}", value, meta);
                let body: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                let mut request = tonic::Request::new(body.clone());
                println!("req: {:?}, body: {:?}", request, body);
                let metadata = request.metadata_mut();
                if let Some(headers) = meta.get_str("headers") {
                    metadata.insert("headers", headers.parse().unwrap());
                } else {
                    metadata.insert("headers", "none".parse().unwrap());
                }
                let resp: tonic::Response<#package_ident::#response_message_ident> = self.inner.get(&String::from(#key_token)).unwrap().#method_ident(request).await.unwrap();
                println!("resp: {:?}", resp);
                let message = tremor_value::to_value(resp.into_inner()).unwrap();
                println!("response serialized: {:?}", message);
                let response_meta = tremor_value::value::Object::with_capacity(1);
                let event: tremor_script::EventPayload = (message, response_meta).into();
                event
            }
        }
    }
}
// 
// fn generate_streaming_client(
    // client_ident: Ident,
    // method_ident: Ident,
    // request_message_ident: Ident,
    // response_message_ident: Ident,
    // package_ident: Ident,
    // client_mod_ident: Ident,
// ) -> TokenStream {
    // quote! {
        // use value_trait::ValueAccess;
        // #[derive(Debug, Clone)]
        // pub struct GrpcClient {
            // inner: hashbrown::HashMap<String, #package_ident::#client_mod_ident::#client_ident<tonic::transport::channel::Channel>>
        // }
// 
        // pub fn structurize(value: tremor_value::Value<'_>) -> #package_ident::#request_message_ident {
            // let message: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
            // message
        // }
// 
        // impl GrpcClient {
            // pub async fn connect(addr: String) -> Self {
                // let inner = #package_ident::#client_mod_ident::#client_ident::connect(addr).await.unwrap();
                // GrpcClient{ inner }
            // }
            // 
            // pub async fn send_streaming_request(&mut self, rx: async_channel::Sender<#package_ident::#request_message_ident>, meta: tremor_value::Value<'_>) -> tremor_script::EventPayload {
                // let (stream_tx, stream_rx) = async_channel::unbounded();
                // let mut request = tonic::Request::new(rx);
                // let reqest_metadata = request.metadata_mut();
                // if let Some(headers) = meta.get_str("headers") {
                    // request_metadata.insert("headers", headers.parse().unwrap());
                // } else {
                    // request_metadata.insert("headers", "none".parse().unwrap());
                // }
                // let resp: tonic::Response<#package_ident::#response_message_ident> = self.inner.record_route(request).await.unwrap();
                // println!("resp: {:?}", resp);
                // let message = tremor_value::to_value(resp.into_inner()).unwrap();
                // println!("response serialized: {:?}", message);
                // let response_meta = tremor_value::value::Object::with_capacity(response_metadata.len());
                // response.insert("resp_header".into(), Value::from(String::from("works")));
                // let event: tremor_script::EventPayload = (message, response_meta).into();
                // event
            // }
        // }
    // }
// }
