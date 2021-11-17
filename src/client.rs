use prost_types::FileDescriptorProto;
use quote::quote;

use proc_macro2::{Ident, TokenStream};

use crate::{FileProtoInfo, MethodProtoInfo, ServiceProtoInfo, };
use std::str::FromStr;

pub fn generate_grpc_client_impl(files: Vec<FileDescriptorProto>) -> String {
    let use_statements = generate_use_statements();
    let grpc_client_handler = generate_grpc_client_handler();
    let client_handler_methods = generate_grpc_client_handler_methods(files.clone());
    let tremor_grpc_client = generate_tremor_grpc_client();
    let tremor_grpc_impl = generate_tremor_grpc_client_impls(files);
    let code = quote! {
        #use_statements
        #grpc_client_handler
        #client_handler_methods
        #tremor_grpc_client
        #tremor_grpc_impl
    };

    let formatted_code = format!("{}", code);
    formatted_code
}

fn generate_use_statements () -> TokenStream {
    quote! {
        use async_std::prelude::StreamExt;
        use value_trait::ValueAccess;
    }
}

fn generate_grpc_client_handler() -> TokenStream {
    quote! {
        #[derive(Debug)]
        pub struct GrpcClientHandler {
            clients: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>>,
            senders: hashbrown::HashMap<u64, async_std::channel::Sender<tremor_value::Value<'static>>>,
            methods: hashbrown::HashMap<String, prost_types::MethodDescriptorProto>,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
        }
    }
}

fn generate_grpc_client_handler_methods(files: Vec<FileDescriptorProto>) -> TokenStream {
    let connect_code = generate_grpc_client_handler_connect(files);
    let send_request_code = generate_client_handler_send_request();
    quote! {
        impl GrpcClientHandler {
            #connect_code
            #send_request_code
        }
    }
}

fn generate_grpc_client_handler_connect(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut clients = vec![];
    let mut methods = vec![];
    for file in files {
        let package_ident = file.package_ident();
        for service in &file.service {
            let client_mod_ident = service.client_mod_ident();
            let client_ident = service.client_ident();
            let client_identifier = quote::format_ident!("{}_{}_{}", package_ident, client_mod_ident, client_ident);
            let client_key = format!("\"{}.{}\"", package_ident, service.name());
            let client_key_token = TokenStream::from_str(&client_key).unwrap();
            clients.push(quote! {
                let #client_identifier = #package_ident::#client_mod_ident::#client_ident::connect(addr.clone()).await.unwrap();
                client_map.insert(String::from(#client_key_token), Box::new(#client_identifier));
            });
            for method in &service.method {
                let method_key = format!("\"{}.{}/{}\"", package_ident, service.name(), method.name());
                let method_key_token = TokenStream::from_str(&method_key).unwrap();
                let method_name_ident = method.name_ident();
                let method_name = TokenStream::from_str(format!("Some(String::from({}))", format!("\"{}\"", method_name_ident)).as_str()).unwrap();
                let input_type_ident = method.request_message_ident(file.package());
                let input_type_name = TokenStream::from_str(format!("Some(String::from({}))", format!("\"{}\"", input_type_ident)).as_str()).unwrap();
                let output_type_ident = method.response_message_ident(file.package());
                let output_type_name = TokenStream::from_str(format!("Some(String::from({}))", format!("\"{}\"", output_type_ident)).as_str()).unwrap();
                let client_streaming = TokenStream::from_str(format!("Some({})", method.client_streaming()).as_str()).unwrap();
                let server_streaming = TokenStream::from_str(format!("Some({})", method.server_streaming()).as_str()).unwrap();
                methods.push(quote! {
                    methods_map.insert(String::from(#method_key_token), prost_types::MethodDescriptorProto {
                        name: #method_name,
                        input_type: #input_type_name,
                        output_type: #output_type_name,
                        client_streaming: #client_streaming,
                        server_streaming: #server_streaming,
                        options: None
                    });
                });
            }
        }
    }
    let mut clients_code = TokenStream::default();
    clients_code.extend(clients);
    let mut methods_code = TokenStream::default();
    methods_code.extend(methods);
    quote! {
        pub async fn connect(addr: String, reply_tx: async_std::channel::Sender<tremor_script::EventPayload>) -> Self {
            let mut client_map: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>> = hashbrown::HashMap::new();
            let mut methods_map: hashbrown::HashMap<String, prost_types::MethodDescriptorProto> = hashbrown::HashMap::new();
            let senders_map: hashbrown::HashMap<u64, async_std::channel::Sender<tremor_value::Value<'static>>> = hashbrown::HashMap::new();

            #clients_code
            #methods_code

            GrpcClientHandler {
                clients: client_map,
                methods: methods_map,
                senders: senders_map,
                reply_tx
            }
        }
    }
}

fn generate_client_handler_send_request() -> TokenStream {
    quote! {
        pub async fn send_request(&mut self, event: tremor_pipeline::Event) {
            let request_path = event.value_meta_iter().next().and_then(|(value, meta)| meta.get_str("request_path"));
            if let Some(path) = request_path {
                let client_key = path.split("/").collect::<Vec<&str>>()[0];
                if let Some((method, client)) = self.methods.get(path).zip(self.clients.get_mut(client_key)) {
                    if !method.client_streaming() && !method.server_streaming() {
                        client.send_unary_request(event, self.reply_tx.clone()).await;
                    }
                    else if method.client_streaming() && !method.server_streaming() {
                        client.send_client_stream_request(event, self.reply_tx.clone(), &mut self.senders).await;
                    }
                }
            }
        }
    }
}

fn generate_tremor_grpc_client() -> TokenStream {
    quote! {
        #[async_trait::async_trait]
        trait TremorGrpcClient: Send  + std::fmt::Debug {
            async fn send_unary_request(
                &mut self,
                event: tremor_pipeline::Event,
                reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
            ) {}
            async fn send_client_stream_request(
                &mut self,
                event: tremor_pipeline::Event,
                reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<tremor_value::Value<'static>>>
            ) {}
            async fn send_server_stream_request(
                &mut self,
                event: tremor_pipeline::Event,
                reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
            ) {}
            async fn send_binary_stream_request(
                &mut self,
                event: tremor_pipeline::Event,
                reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<tremor_value::Value>>
            ) {}
        }
    }
}

fn generate_tremor_grpc_client_impls(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut trait_impls = vec![];
    for file in files {
        let package_ident = file.package_ident();
        for service in &file.service {
            let client_mod_ident = service.client_mod_ident();
            let client_ident = service.client_ident();
            let mut send_unary_request_code = TokenStream::default();
            let mut send_client_stream_request_code = TokenStream::default();
            for method in &service.method {
                let request_message_ident = method.request_message_ident(file.package());
                let response_message_ident = method.response_message_ident(file.package());
                let method_ident = method.name_ident();
                if !method.server_streaming() && !method.client_streaming() {
                    send_unary_request_code = generate_send_unary_request(package_ident.clone(), method_ident.clone(), request_message_ident.clone(), response_message_ident.clone());
                } 
                if !method.server_streaming() && method.client_streaming() {
                    send_client_stream_request_code = generate_send_client_stream_request(package_ident.clone(), method_ident.clone(), request_message_ident.clone(), response_message_ident.clone());
                }
            }
            trait_impls.push(quote! {
                #[async_trait::async_trait]
                impl TremorGrpcClient for #package_ident::#client_mod_ident::#client_ident<tonic::transport::Channel> {
                    #send_unary_request_code

                    #send_client_stream_request_code
                }
            });
        }
    }
    let mut code = TokenStream::default();
    code.extend(trait_impls);
    code
}

fn generate_send_unary_request(package_ident: Ident, method_ident: Ident, request_message_ident: Ident, response_message_ident: Ident) -> TokenStream {
    quote! {
        async fn send_unary_request(&mut self, event: tremor_pipeline::Event, reply_tx: async_std::channel::Sender<tremor_script::EventPayload>) {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    println!("args: {:?} {:?}", value, meta);
                    let body: #package_ident::#request_message_ident = tremor_value::structurize(value.clone_static()).unwrap();
                    let mut request = tonic::Request::new(body.clone());
                    println!("req: {:?}, body: {:?}", request, body);
                    let metadata = request.metadata_mut();
                    if let Some(headers) = meta.get_str("headers") {
                        metadata.insert("headers", headers.parse().unwrap());
                    } else {
                        metadata.insert("headers", "none".parse().unwrap());
                    }
                    let resp: tonic::Response<#package_ident::#response_message_ident> = self.#method_ident(request).await.unwrap();
                    println!("resp: {:?}", resp);
                    let message = tremor_value::to_value(resp.into_inner()).unwrap();
                    println!("response serialized: {:?}", message);
                    let response_meta = tremor_value::value::Object::with_capacity(1);
                    let event: tremor_script::EventPayload = (message, response_meta).into();
                    reply_tx.send(event).await.unwrap();
                }
            } 
        }
    }
}

fn generate_send_client_stream_request(package_ident: Ident, method_ident: Ident, request_message_ident: Ident, response_message_ident: Ident) -> TokenStream {
    quote! {
        async fn send_client_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<tremor_value::Value<'static>>>) 
        {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    if let Some(stream_id) = meta.get_u64("stream_id") {
                        if let Some(tx) = senders.get(&stream_id) {
                            tx.send(value.clone_static()).await.unwrap();
                            if let Some(flag) = meta.get_str("flag") {
                                tx.close();
                            } 
                        } else {
                            let (tx, rx) = async_std::channel::unbounded::<tremor_value::Value<'static>>();
                            senders.insert(stream_id.clone(), tx.clone());
                            // fn smth(value: tremor_value::Value<'static>) -> #package_ident::#request_message_ident {
                                // let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                                // structured_value
                            // }

                            fn mat() -> impl FnMut(tremor_value::Value<'static>) -> #package_ident::#request_message_ident {
                                |value: tremor_value::Value<'static>| {
                                    let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                                    structured_value
                                }
                            }
                            // let more: dyn FnMut(tremor_value::Value<'static>) ->  #package_ident::#request_message_ident = |value: tremor_value::Value<'static>| {
                                // let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                                // structured_value
                            // };
                            // let rx = rx.map(|value: tremor_value::Value<'static>| {
                                // let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                                // structured_value
                            // });
                            let rx = rx.map(mat());
                            let mut request = tonic::Request::new(rx);
                            let metadata = request.metadata_mut();
                            if let Some(headers) = meta.get_str("headers") {
                                metadata.insert("headers", headers.parse().unwrap());
                            } else {
                                metadata.insert("headers", "none".parse().unwrap());
                            }
                            async_std::task::spawn(async move {
                                tx.send(value.clone_static()).await.unwrap();
                            });
                            let resp: tonic::Response<#package_ident::#response_message_ident> = self.#method_ident(request).await.unwrap();
                            println!("resp: {:?}", resp);
                            let message = tremor_value::to_value(resp.into_inner()).unwrap();
                            println!("response serialized: {:?}", message);
                            let response_meta = tremor_value::value::Object::with_capacity(1);
                            let event: tremor_script::EventPayload = (message, response_meta).into();
                            reply_tx.send(event).await.unwrap();
                        }
                    } 
                }
            }
        }
    }
}

// fn generate_client_handler_handle_unary_request() {
    // quote! {
        // async fn handle_unary_request(&mut self, value: tremor_value::Value<'_>, meta: tremor_value::Value<'_>) -> tremor_script::EventPayload {
            // println!("args: {:?} {:?}", value, meta);
            // let body: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
            // let mut request = tonic::Request::new(body.clone());
            // println!("req: {:?}, body: {:?}", request, body);
            // let metadata = request.metadata_mut();
            // if let Some(headers) = meta.get_str("headers") {
                // metadata.insert("headers", headers.parse().unwrap());
            // } else {
                // metadata.insert("headers", "none".parse().unwrap());
            // }
            // let resp: tonic::Response<#package_ident::#response_message_ident> = self.inner.get(&String::from(#key_token)).unwrap().#method_ident(request).await.unwrap();
        // }
    // }
// }


// fn generate_unary_client(
    // client_ident: Ident,
    // method_ident: Ident,
    // request_message_ident: Ident,
    // response_message_ident: Ident,
    // package_ident: Ident,
    // client_mod_ident: Ident,
// ) -> TokenStream {
    // let key_str = format!("\"{}\"", package_ident);
    // let key_token = TokenStream::from_str(&key_str).unwrap();
    // quote! {
        // use value_trait::ValueAccess;
        // #[derive(Debug, Clone)]
        // pub struct GrpcClient {
            // inner: hashbrown::HashMap<String, Box<dyn Send>>,
        // }
// 
        // impl GrpcClient {
            // pub async fn connect(addr: String) -> Self {
                // let client = #package_ident::#client_mod_ident::#client_ident::connect(addr).await.unwrap();
                // let inner = hashbrown::HashMap::new();
                // inner.insert(String::from(#key_token), Box::new(client));
                // GrpcClient{ inner }
            // }
// 
            // pub async fn send_request(&mut self, value: tremor_value::Value<'_>, meta: tremor_value::Value<'_>) -> tremor_script::EventPayload {
                // println!("args: {:?} {:?}", value, meta);
                // let body: #package_ident::#request_message_ident = tremor_value::structurize(value).unwrap();
                // let mut request = tonic::Request::new(body.clone());
                // println!("req: {:?}, body: {:?}", request, body);
                // let metadata = request.metadata_mut();
                // if let Some(headers) = meta.get_str("headers") {
                    // metadata.insert("headers", headers.parse().unwrap());
                // } else {
                    // metadata.insert("headers", "none".parse().unwrap());
                // }
                // let resp: tonic::Response<#package_ident::#response_message_ident> = self.inner.get(&String::from(#key_token)).unwrap().#method_ident(request).await.unwrap();
                // println!("resp: {:?}", resp);
                // let message = tremor_value::to_value(resp.into_inner()).unwrap();
                // println!("response serialized: {:?}", message);
                // let response_meta = tremor_value::value::Object::with_capacity(1);
                // let event: tremor_script::EventPayload = (message, response_meta).into();
                // event
            // }
        // }
    // }
// }
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
            // pub async fn send_streaming_request(&mut self, rx: async_std::channel::Sender<#package_ident::#request_message_ident>, meta: tremor_value::Value<'_>) -> tremor_script::EventPayload {
                // let (stream_tx, stream_rx) = async_std::channel::unbounded();
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
