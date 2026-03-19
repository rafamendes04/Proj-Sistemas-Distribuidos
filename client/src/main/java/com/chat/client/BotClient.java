package com.chat.client;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import com.chat.proto.ChatProto.Wrapper;
import com.chat.proto.ChatProto.LoginRequest;
import com.chat.proto.ChatProto.CreateChannelRequest;
import com.chat.proto.ChatProto.ListChannelsRequest;
import com.chat.proto.ChatProto.MessageType;
import com.chat.proto.ChatProto.Status;

import java.util.UUID;

public class BotClient {
    public static void main(String[] args) throws InterruptedException {
        // As configurações externas permitem flexibilidade nos containers Docker
        String brokerUrl = System.getenv("BROKER_URL");
        if (brokerUrl == null) brokerUrl = "tcp://broker:5555";
        
        String botName = System.getenv("BOT_NAME");
        if (botName == null) botName = "Bot_" + UUID.randomUUID().toString().substring(0, 5);

        System.out.println("🤖 [" + botName + "] Iniciando. Conectando ao broker em " + brokerUrl);

        try (ZContext context = new ZContext()) {
            // Padrão REQ para criar pacotes RPC para o servidor -> Aguarda obrigatoriamente um REP
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.setReceiveTimeOut(5000); // Timeout de 5s para não travar a thread infinitamente
            socket.connect(brokerUrl);

            // 1. Loop contínuo tentando Logar no bot server
            boolean loggedIn = false;
            while (!loggedIn) {
                System.out.println("🤖 [" + botName + "] Tentando realizar login...");
                Wrapper loginReq = Wrapper.newBuilder()
                    .setType(MessageType.LOGIN_REQ)
                    .setTimestamp(System.currentTimeMillis())
                    .setLoginReq(LoginRequest.newBuilder().setUsername(botName).build())
                    .build();

                // Transforma as instâncias da classe gerada pelo Protobuf em bytes para viajar no ZeroMQ
                socket.send(loginReq.toByteArray(), 0);
                
                byte[] reply = socket.recv(0);
                if (reply == null) {
                    System.out.println("⚠️ [" + botName + "] Tempo esgotado aguardando resposta do login. Retentando...");
                    // Ao dar timeout, o padrão REQ/REP oficial exige recrear ou fechar o socket pra manter a consistência da FSM Interna
                    socket.close();
                    socket = context.createSocket(SocketType.REQ);
                    socket.setReceiveTimeOut(5000);
                    socket.connect(brokerUrl);
                    Thread.sleep(2000);
                    continue; // Repete até conseguir login
                }
                
                try {
                    Wrapper resp = Wrapper.parseFrom(reply);
                    if (resp.getType() == MessageType.LOGIN_RESP) {
                        if (resp.getLoginResp().getStatus() == Status.SUCCESS) {
                            System.out.println("✅ [" + botName + "] Login bem sucedido! Msg: " + resp.getLoginResp().getMessage());
                            loggedIn = true;
                        } else {
                            System.out.println("❌ [" + botName + "] Falha no login: " + resp.getLoginResp().getMessage());
                            Thread.sleep(2000);
                        }
                    } else {
                        System.out.println("⚠️ [" + botName + "] Tipo de resposta inesperada recebida: " + resp.getType());
                    }
                } catch (Exception e) {
                    System.err.println("❌ [" + botName + "] Erro interpretando payload protobuf: " + e.getMessage());
                }
            }

            // 2. Após logar, requisita quais canais estão disponíveis
            System.out.println("📄 [" + botName + "] Solicitando listagem de canais...");
            Wrapper listReq = Wrapper.newBuilder()
                .setType(MessageType.LIST_CHANNELS_REQ)
                .setTimestamp(System.currentTimeMillis())
                .setListChannelsReq(ListChannelsRequest.newBuilder().build())
                .build();
            
            socket.send(listReq.toByteArray(), 0);
            byte[] listReplyRaw = socket.recv(0);
            if (listReplyRaw != null) {
                try {
                    Wrapper resp = Wrapper.parseFrom(listReplyRaw);
                    if (resp.getType() == MessageType.LIST_CHANNELS_RESP && resp.getListChannelsResp().getStatus() == Status.SUCCESS) {
                        System.out.println("📚 [" + botName + "] Canais atualmente disponíveis: " + resp.getListChannelsResp().getChannelsList());
                    }
                } catch (Exception e) {
                    System.err.println("❌ [" + botName + "] Erro lendo listagem de canais: " + e.getMessage());
                }
            }

            // 3. Em seguida, cria o próprio canal do bot garantindo que o nome do canal seja único e exlusivo para ele
            String myChannel = "channel_" + botName;
            System.out.println("➕ [" + botName + "] Requisição para criação do novo canal: " + myChannel);
            Wrapper createReq = Wrapper.newBuilder()
                .setType(MessageType.CREATE_CHANNEL_REQ)
                .setTimestamp(System.currentTimeMillis())
                .setCreateChannelReq(CreateChannelRequest.newBuilder().setChannelName(myChannel).build())
                .build();
            
            socket.send(createReq.toByteArray(), 0);
            byte[] createReplyRaw = socket.recv(0);
            if (createReplyRaw != null) {
                try {
                    Wrapper resp = Wrapper.parseFrom(createReplyRaw);
                    if (resp.getType() == MessageType.CREATE_CHANNEL_RESP) {
                        if (resp.getCreateChannelResp().getStatus() == Status.SUCCESS) {
                            System.out.println("✅ [" + botName + "] Sucesso criando canal: " + resp.getCreateChannelResp().getMessage());
                        } else {
                            System.out.println("❌ [" + botName + "] Server negou criar o canal: " + resp.getCreateChannelResp().getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("❌ [" + botName + "] Retorno inesperado na criação: " + e.getMessage());
                }
            }
            
            // 4. Manter bot "vivo" solicitando atualizações infinitamente
            while(true) {
                Thread.sleep(8000); // Pausa 8s
                System.out.println("🔍 [" + botName + "] (Rotina) Listando updates dos canais...");
                
                // Re-enviar a mesma requisição imutável já instanciada listReq
                
                // Timeout sanity check before reuse in REQ
                socket.send(listReq.toByteArray(), 0);
                byte[] periodicReply = socket.recv(0);
                if (periodicReply != null) {
                    try {
                        Wrapper resp = Wrapper.parseFrom(periodicReply);
                        if(resp.getType() == MessageType.LIST_CHANNELS_RESP) {
                            System.out.println("📚 [" + botName + "] Status dos canais online: " + resp.getListChannelsResp().getChannelsList().toString());
                        }
                    } catch (Exception e) {}
                } else {
                    // Timeout handled silently or recover socket
                    socket.close();
                    socket = context.createSocket(SocketType.REQ);
                    socket.setReceiveTimeOut(5000);
                    socket.connect(brokerUrl);
                }
            }
        }
    }
}
