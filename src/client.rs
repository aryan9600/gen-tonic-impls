use prost_types::FileDescriptorProto;
use quote::quote;

use proc_macro2::{Ident, Literal, TokenStream};

use crate::{FileProtoInfo, MethodProtoInfo, ServiceProtoInfo, };

pub fn generate_grpc_client_impl(files: Vec<FileDescriptorProto>) -> String {
    let use_statements = generate_use_statements();
    let static_value_struct = generate_static_value_struct();
    let grpc_client_handler = generate_grpc_client_handler();
    let client_handler_methods = generate_grpc_client_handler_methods(files.clone());
    let tremor_grpc_client = generate_tremor_grpc_client();
    let tremor_grpc_impl = generate_tremor_grpc_client_impls(files);
    let code = quote! {
        #use_statements
        #static_value_struct
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
        use crate::errors::{Error, ErrorKind, Result};
    }
}

fn generate_static_value_struct() -> TokenStream {
    quote! {
        struct StaticValue(tremor_value::Value<'static>);
    }
}

fn generate_grpc_client_handler() -> TokenStream {
    quote! {
        #[derive(Debug)]
        pub struct GrpcClientHandler {
            clients: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>>,
            senders: hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>,
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
            let client_identifier = quote::format_ident!("{}_{}", package_ident, client_mod_ident);
            let client_key_token = Literal::string(&format!("{}.{}", package_ident, service.name()));
            clients.push(quote! {
                let #client_identifier = #package_ident::#client_mod_ident::#client_ident::connect(addr.clone()).await?;
                client_map.insert(String::from(#client_key_token), Box::new(#client_identifier));
            });
            for method in &service.method {
                let method_key_token = Literal::string(&format!("{}.{}/{}", package_ident, service.name(), method.name()));
                let method_name = Literal::string(method.name());
                let method_name_expr = quote! {Some(String::from(#method_name))}; 
                let input_type_name = Literal::string(method.request_message_ident(file.package()).to_string().as_str());
                let input_type_expr = quote! {Some(String::from(#input_type_name))};
                let output_type_name = Literal::string(method.response_message_ident(file.package()).to_string().as_str());
                let output_type_expr = quote! {Some(String::from(#output_type_name))};
                let client_streaming = quote::format_ident!("{}", method.client_streaming());
                let client_streaming_expr = quote! {Some(#client_streaming)};
                let server_streaming = quote::format_ident!("{}", method.server_streaming());
                let server_streaming_expr = quote! {Some(#server_streaming)};
                methods.push(quote! {
                    methods_map.insert(String::from(#method_key_token), prost_types::MethodDescriptorProto {
                        name: #method_name_expr,
                        input_type: #input_type_expr,
                        output_type: #output_type_expr,
                        client_streaming: #client_streaming_expr,
                        server_streaming: #server_streaming_expr,
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
        pub async fn connect(addr: String, reply_tx: async_std::channel::Sender<tremor_script::EventPayload>) -> crate::errors::Result<Self> {
            let mut client_map: hashbrown::HashMap<String, Box<dyn TremorGrpcClient>> = hashbrown::HashMap::new();
            let mut methods_map: hashbrown::HashMap<String, prost_types::MethodDescriptorProto> = hashbrown::HashMap::new();
            let senders_map: hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>> = hashbrown::HashMap::new();

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
            let request_path = event.value_meta_iter().next().and_then(|(_, meta)| meta.get_str("request_path"));
            if let Some(path) = request_path {
                let client_key = path.split("/").collect::<Vec<&str>>()[0];
                if let Some((method, client)) = self.methods.get(path).zip(self.clients.get_mut(client_key)) {
                    if !method.client_streaming() && !method.server_streaming() {
                        client.send_unary_request(event, self.reply_tx.clone()).await;
                    }
                    else if method.client_streaming() && !method.server_streaming() {
                        client.send_client_stream_request(event, self.reply_tx.clone(), &mut self.senders).await;
                    } else if !method.client_streaming() && method.server_streaming() {
                        client.send_server_stream_request(event, self.reply_tx.clone()).await;
                    } else {
                        client.send_binary_stream_request(event, self.reply_tx.clone(), &mut self.senders).await;
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
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_client_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_server_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
            ) -> crate::errors::Result<()> {
                Ok(())
            }
            async fn send_binary_stream_request(
                &mut self,
                _event: tremor_pipeline::Event,
                _reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
                _senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>
            ) -> crate::errors::Result<()> {
                Ok(())
            }
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
            let mut send_server_stream_request_code = TokenStream::default();
            let mut send_binary_stream_request_code = TokenStream::default();
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
                if method.server_streaming() && !method.client_streaming() {
                    send_server_stream_request_code = generate_send_server_stream_request(package_ident.clone(), method_ident.clone(), request_message_ident.clone(), response_message_ident.clone());
                }
                if method.server_streaming() && method.client_streaming() {
                    send_binary_stream_request_code = generate_send_binary_stream_request(package_ident.clone(), method_ident.clone(), request_message_ident.clone(), response_message_ident.clone());
                }
            }
            trait_impls.push(quote! {
                #[async_trait::async_trait]
                impl TremorGrpcClient for #package_ident::#client_mod_ident::#client_ident<tonic::transport::Channel> {
                    #send_unary_request_code

                    #send_client_stream_request_code

                    #send_server_stream_request_code

                    #send_binary_stream_request_code
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
        async fn send_unary_request(&mut self, event: tremor_pipeline::Event, reply_tx: async_std::channel::Sender<tremor_script::EventPayload>) -> crate::errors::Result<()> {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    println!("args: {:?} {:?}", value, meta);
                    let body: #package_ident::#request_message_ident = tremor_value::structurize(value.clone_static())?;
                    let mut request = tonic::Request::new(body.clone());
                    println!("req: {:?}, body: {:?}", request, body);
                    let metadata = request.metadata_mut();
                    if let Some(headers) = meta.get_str("headers") {
                        metadata.insert("headers", headers.parse().unwrap());
                    } else {
                        metadata.insert("headers", "none".parse().unwrap());
                    }
                    let resp: tonic::Response<#package_ident::#response_message_ident> = self.#method_ident(request).await?;
                    println!("resp: {:?}", resp);
                    let message = tremor_value::to_value(resp.into_inner())?;
                    println!("response serialized: {:?}", message);
                    let response_meta = tremor_value::value::Object::with_capacity(1);
                    let event: tremor_script::EventPayload = (message, response_meta).into();
                    reply_tx.send(event).await?;
                }
            } 
            Ok(())
        }
    }
}

fn generate_send_client_stream_request(package_ident: Ident, method_ident: Ident, request_message_ident: Ident, response_message_ident: Ident) -> TokenStream {
    quote! {
        async fn send_client_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>
        ) {
            if !event.is_batch {
                for (value, meta) in event.value_meta_iter() {
                    let mut client = self.clone();
                    let reply_tx = reply_tx.clone();
                    let value = value.clone_static();
                    let meta = meta.clone_static();
                    println!("value: {:?}, meta: {:?} \n", value, meta);
                    if let (stream_id) = meta.get_u64("stream_id") {
                        println!("stream_id: {:?}", stream_id);
                        if let Some(tx) = senders.get(&stream_id) {
                            println!("tx: {:?}", tx);
                            tx.send(StaticValue(value)).await.unwrap();
                            if meta.contains_key("flag") {
                                println!("flagggg");
                                tx.close();
                            } 
                        } else {
                            let (tx, rx) = async_std::channel::unbounded::<StaticValue>();
                            senders.insert(stream_id.clone(), tx.clone());
                            println!("senders: {:?}", &senders);
                            let rx = rx.map(|val| {
                                let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(val.0).unwrap();
                                structured_value
                            });
                            // let rx = rx.map(mat());
                            let mut request = tonic::Request::new(rx);
                            let metadata = request.metadata_mut();
                            if let Some(headers) = meta.get_str("headers") {
                                metadata.insert("headers", headers.parse().unwrap());
                            } else {
                                metadata.insert("headers", "none".parse().unwrap());
                            }
                            async_std::task::spawn(async move {
                                tx.send(StaticValue(value)).await.unwrap();
                            });
                            async_std::task::spawn(async move {
                                let resp: tonic::Response<#package_ident::#response_message_ident> = client.#method_ident(request).await.unwrap();
                                println!("resp: {:?}", resp);
                                let message = tremor_value::to_value(resp.into_inner()).unwrap();
                                println!("response serialized: {:?}", message);
                                let response_meta = tremor_value::value::Object::with_capacity(1);
                                let response_event: tremor_script::EventPayload = (message, response_meta).into();
                                reply_tx.send(response_event).await.unwrap();
                            });
                        }
                    } 
                }
            }
            Ok(())
        }
    }
}

fn generate_send_server_stream_request(package_ident: Ident, method_ident: Ident, request_message_ident: Ident, response_message_ident: Ident) -> TokenStream {
    quote! {
        async fn send_server_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>
        ) {
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
                let resp: tonic::Response<tonic::Streaming<#package_ident::#response_message_ident>> = self.#method_ident(request).await.unwrap();
                println!("resp: {:?}", resp);
                let mut stream = resp.into_inner();
                while let Some(item) = stream.message().await.unwrap() {
                    println!("item: {:?}", item);
                    let message = tremor_value::to_value(item).unwrap();
                    println!("response serialized: {:?}", message);
                    let response_meta = tremor_value::value::Object::with_capacity(1);
                    let event: tremor_script::EventPayload = (message, response_meta).into();
                    reply_tx.send(event).await.unwrap();
                }
            }
            Ok(())
        }
    }
}

fn generate_send_binary_stream_request(package_ident: Ident, method_ident: Ident, request_message_ident: Ident, response_message_ident: Ident) -> TokenStream {
    quote! {
        async fn send_binary_stream_request(
            &mut self,
            event: tremor_pipeline::Event,
            reply_tx: async_std::channel::Sender<tremor_script::EventPayload>,
            senders: &mut hashbrown::HashMap<u64, async_std::channel::Sender<StaticValue>>
        ) {
            for (value, meta) in event.value_meta_iter() {
                let mut client = self.clone();
                let reply_tx = reply_tx.clone();
                let value = value.clone_static();
                let meta = meta.clone_static();
                println!("value: {:?}, meta: {:?} \n", value, meta);
                if let Some(stream_id) = meta.get_u64("stream_id") {
                    println!("stream_id: {:?}", stream_id);
                    if let Some(tx) = senders.get(&stream_id) {
                        println!("tx: {:?}", tx);
                        tx.send(StaticValue(value)).await.unwrap();
                        if let Some(_flag) = meta.get_u64("flag") {
                            println!("flagggg");
                            tx.close();
                        } 
                    } else {
                        let (tx, rx) = async_std::channel::unbounded::<StaticValue>();
                        senders.insert(stream_id.clone(), tx.clone());
                        println!("senders: {:?}", &senders);
                        let rx = rx.map(|val| {
                            let structured_value: #package_ident::#request_message_ident = tremor_value::structurize(val.0).unwrap();
                            structured_value
                        });
                        let mut request = tonic::Request::new(rx);
                        let metadata = request.metadata_mut();
                        if let Some(headers) = meta.get_str("headers") {
                            metadata.insert("headers", headers.parse().unwrap());
                        } else {
                            metadata.insert("headers", "none".parse().unwrap());
                        }
                        async_std::task::spawn(async move {
                            tx.send(StaticValue(value)).await.unwrap();
                        });
                        async_std::task::spawn(async move {
                            let resp: tonic::Response<tonic::Streaming<#package_ident::#response_message_ident>> = client.#method_ident(request).await.unwrap();
                            println!("resp: {:?}", resp);
                            let mut stream = resp.into_inner();
                            while let Some(item) = stream.message().await.unwrap() {
                                println!("item: {:?}", item);
                                let message = tremor_value::to_value(item).unwrap();
                                println!("response serialized: {:?}", message);
                                let response_meta = tremor_value::value::Object::with_capacity(1);
                                let event: tremor_script::EventPayload = (message, response_meta).into();
                                reply_tx.send(event).await.unwrap();
                            }
                        });
                    }
                } 
            }
            Ok(())
        }
    }
}
