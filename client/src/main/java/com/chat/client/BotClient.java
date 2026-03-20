package com.chat.client;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BotClient {
    public static void main(String[] args) throws InterruptedException {
        // As configurações externas permitem flexibilidade nos containers Docker
        String brokerUrl = System.getenv("BROKER_URL");
        if (brokerUrl == null) brokerUrl = "tcp://broker:5555";
        
        String botName = System.getenv("BOT_NAME");
        if (botName == null) botName = "Bot_" + UUID.randomUUID().toString().substring(0, 5);

        System.out.println("🤖 [" + botName + "] Iniciando. Conectando ao broker em " + brokerUrl);

        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.setReceiveTimeOut(5000);
            socket.connect(brokerUrl);

            boolean loggedIn = false;
            while (!loggedIn) {
                System.out.println("🤖 [" + botName + "] Tentando realizar login...");
                
                Map<String, Object> loginReq = new HashMap<>();
                loginReq.put("type", "LOGIN_REQ");
                loginReq.put("timestamp", System.currentTimeMillis());
                
                Map<String, Object> payload = new HashMap<>();
                payload.put("username", botName);
                loginReq.put("payload", payload);

                try {
                    System.out.println("🤖 [" + botName + "] --> Enviando Login | Conteúdo Completo: " + loginReq);
                    socket.send(objectMapper.writeValueAsBytes(loginReq), 0);
                    
                    byte[] reply = socket.recv(0);
                    if (reply == null) {
                        System.out.println("⚠️ [" + botName + "] Tempo esgotado aguardando resposta do login. Retentando...");
                        socket.close();
                        socket = context.createSocket(SocketType.REQ);
                        socket.setReceiveTimeOut(5000);
                        socket.connect(brokerUrl);
                        Thread.sleep(2000);
                        continue;
                    }

                    Map<String, Object> resp = objectMapper.readValue(reply, Map.class);
                    System.out.println("🤖 [" + botName + "] <-- Resposta de Login Recebida | Conteúdo Completo: " + resp);

                    if ("LOGIN_RESP".equals(resp.get("type"))) {
                        Map<String, Object> respPayload = (Map<String, Object>) resp.get("payload");
                        if ("SUCCESS".equals(respPayload.get("status"))) {
                            System.out.println("✅ [" + botName + "] Login bem sucedido! Msg: " + respPayload.get("message"));
                            loggedIn = true;
                        } else {
                            System.out.println("❌ [" + botName + "] Falha no login: " + respPayload.get("message"));
                            Thread.sleep(2000);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("❌ [" + botName + "] Erro: " + e.getMessage());
                }
            }

            System.out.println("📄 [" + botName + "] Solicitando listagem de canais...");
            try {
                Map<String, Object> listReq = new HashMap<>();
                listReq.put("type", "LIST_CHANNELS_REQ");
                listReq.put("timestamp", System.currentTimeMillis());
                listReq.put("payload", new HashMap<>());

                System.out.println("📄 [" + botName + "] --> Enviando Listagem | Conteúdo Completo: " + listReq);
                socket.send(objectMapper.writeValueAsBytes(listReq), 0);
                
                byte[] listReplyRaw = socket.recv(0);
                if (listReplyRaw != null) {
                    Map<String, Object> resp = objectMapper.readValue(listReplyRaw, Map.class);
                    System.out.println("📄 [" + botName + "] <-- Resposta Listagem Recebida | Conteúdo Completo: " + resp);
                    
                    if ("LIST_CHANNELS_RESP".equals(resp.get("type"))) {
                        Map<String, Object> p = (Map<String, Object>) resp.get("payload");
                        if ("SUCCESS".equals(p.get("status"))) {
                            System.out.println("📚 [" + botName + "] Canais atualmente disponíveis: " + p.get("channels"));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("❌ [" + botName + "] Erro: " + e.getMessage());
            }

            String myChannel = "channel_" + botName;
            System.out.println("➕ [" + botName + "] Requisição para criação do novo canal: " + myChannel);
            try {
                Map<String, Object> createReq = new HashMap<>();
                createReq.put("type", "CREATE_CHANNEL_REQ");
                createReq.put("timestamp", System.currentTimeMillis());
                Map<String, Object> payloadCreate = new HashMap<>();
                payloadCreate.put("channel_name", myChannel);
                createReq.put("payload", payloadCreate);

                System.out.println("➕ [" + botName + "] --> Enviando Criação | Conteúdo Completo: " + createReq);
                socket.send(objectMapper.writeValueAsBytes(createReq), 0);
                
                byte[] createReplyRaw = socket.recv(0);
                if (createReplyRaw != null) {
                    Map<String, Object> resp = objectMapper.readValue(createReplyRaw, Map.class);
                    System.out.println("➕ [" + botName + "] <-- Resposta Criação Recebida | Conteúdo Completo: " + resp);

                    if ("CREATE_CHANNEL_RESP".equals(resp.get("type"))) {
                        Map<String, Object> p = (Map<String, Object>) resp.get("payload");
                        if ("SUCCESS".equals(p.get("status"))) {
                            System.out.println("✅ [" + botName + "] Sucesso criando canal: " + p.get("message"));
                        } else {
                            System.out.println("❌ [" + botName + "] Server negou criar o canal: " + p.get("message"));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("❌ [" + botName + "] Erro: " + e.getMessage());
            }

            // Manter bot "vivo" solicitando atualizações infinitamente
            while(true) {
                Thread.sleep(8000); // Pausa 8s
                System.out.println("🔍 [" + botName + "] (Rotina) Listando updates dos canais...");
                
                try {
                    Map<String, Object> updateReq = new HashMap<>();
                    updateReq.put("type", "LIST_CHANNELS_REQ");
                    updateReq.put("timestamp", System.currentTimeMillis()); // Atualiza o timestamp a cada iteração!
                    updateReq.put("payload", new HashMap<>());

                    System.out.println("🔍 [" + botName + "] --> Enviando Rotina | Conteúdo Completo: " + updateReq);
                    socket.send(objectMapper.writeValueAsBytes(updateReq), 0);
                    
                    byte[] periodicReply = socket.recv(0);
                    if (periodicReply != null) {
                        Map<String, Object> resp = objectMapper.readValue(periodicReply, Map.class);
                        System.out.println("🔍 [" + botName + "] <-- Update Recebido | Conteúdo Completo: " + resp);

                        if("LIST_CHANNELS_RESP".equals(resp.get("type"))) {
                            Map<String, Object> p = (Map<String, Object>) resp.get("payload");
                            System.out.println("📚 [" + botName + "] Status dos canais online: " + p.get("channels"));
                        }
                    } else {
                        socket.close();
                        socket = context.createSocket(SocketType.REQ);
                        socket.setReceiveTimeOut(5000);
                        socket.connect(brokerUrl);
                    }
                } catch (Exception e) {
                    System.err.println("❌ [" + botName + "] Erro no loop de update: " + e.getMessage());
                }
            }
        }
    }
}
