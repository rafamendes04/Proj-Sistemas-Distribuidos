package com.chat.client;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.util.*;

public class BotClient {

    static ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
    static ZMQ.Socket reqSocket;
    static ZContext context;
    static String botName;
    static String brokerUrl;

    public static void main(String[] args) throws Exception {
        brokerUrl = System.getenv("BROKER_URL") != null ? System.getenv("BROKER_URL") : "tcp://broker:5555";
        String pubsubUrl = System.getenv("PUBSUB_URL") != null ? System.getenv("PUBSUB_URL") : "tcp://pubsub-proxy:5558";
        botName = System.getenv("BOT_NAME") != null ? System.getenv("BOT_NAME") : "bot_" + UUID.randomUUID().toString().substring(0, 5);

        System.out.println("[" + botName + "] Iniciando. Broker=" + brokerUrl + " | PubSub=" + pubsubUrl);

        context = new ZContext();
        reqSocket = criarSocket();

        while (!fazerLogin()) {
            Thread.sleep(2000);
        }

        List<String> canais = listarCanais();

        if (canais.size() < 5) {
            String novoCanal = "canal_" + botName;
            criarCanal(novoCanal);
            canais = listarCanais();
        }

        ZMQ.Socket subSocket = context.createSocket(SocketType.SUB);
        subSocket.connect(pubsubUrl);

        List<String> inscrito = new ArrayList<>();
        Random rand = new Random();

        List<String> disponiveis = new ArrayList<>(canais);
        Collections.shuffle(disponiveis);
        for (String canal : disponiveis) {
            if (inscrito.size() >= 3) break;
            if (!inscrito.contains(canal)) {
                subSocket.subscribe(canal.getBytes());
                inscrito.add(canal);
                System.out.println("[" + botName + "] Inscrito em: " + canal);
            }
        }

        Thread listener = new Thread(() -> escutarMensagens(subSocket));
        listener.setDaemon(true);
        listener.start();

        String[] frases = {
            "oi galera", "alguem ai?", "tudo bem?", "que dia e hoje",
            "acabei de chegar", "vamos conversar", "boa tarde a todos",
            "alguma novidade?", "to passando so pra ver", "ate mais"
        };

        while (true) {
            canais = listarCanais();
            if (canais.isEmpty()) {
                Thread.sleep(3000);
                continue;
            }

            for (String canal : canais) {
                if (inscrito.size() >= 3) break;
                if (!inscrito.contains(canal)) {
                    subSocket.subscribe(canal.getBytes());
                    inscrito.add(canal);
                    System.out.println("[" + botName + "] Inscrito em: " + canal);
                }
            }

            String canalEscolhido = canais.get(rand.nextInt(canais.size()));

            for (int i = 0; i < 10; i++) {
                String texto = frases[rand.nextInt(frases.length)] + " [" + (i + 1) + "/10]";
                publicarMensagem(canalEscolhido, texto);
                Thread.sleep(1000);
            }
        }
    }

    static void escutarMensagens(ZMQ.Socket sub) {
        while (true) {
            try {
                byte[] topico = sub.recv(0);
                byte[] corpo = sub.recv(0);

                Map<String, Object> msg = mapper.readValue(corpo, Map.class);
                long tsEnvio = ((Number) msg.get("timestamp")).longValue();
                long tsRecebido = System.currentTimeMillis();

                System.out.println("[" + botName + "] RECEBIDO | canal=" + msg.get("channel")
                    + " | de=" + msg.get("username")
                    + " | msg=" + msg.get("message")
                    + " | enviado=" + tsEnvio + " | recebido=" + tsRecebido);
            } catch (Exception e) {
                System.err.println("[" + botName + "] Erro ao receber mensagem: " + e.getMessage());
            }
        }
    }

    static boolean fazerLogin() {
        try {
            Map<String, Object> req = new HashMap<>();
            req.put("type", "LOGIN_REQ");
            req.put("timestamp", System.currentTimeMillis());
            req.put("payload", Map.of("username", botName));

            System.out.println("[" + botName + "] --> LOGIN_REQ");
            reqSocket.send(mapper.writeValueAsBytes(req), 0);

            byte[] raw = reqSocket.recv(0);
            if (raw == null) {
                System.out.println("[" + botName + "] Timeout no login, tentando de novo...");
                reqSocket = criarSocket();
                return false;
            }

            Map<String, Object> resp = mapper.readValue(raw, Map.class);
            Map<String, Object> payload = (Map<String, Object>) resp.get("payload");
            System.out.println("[" + botName + "] <-- LOGIN_RESP | " + payload.get("message"));

            return "SUCCESS".equals(payload.get("status"));
        } catch (Exception e) {
            System.err.println("[" + botName + "] Erro no login: " + e.getMessage());
            reqSocket = criarSocket();
            return false;
        }
    }

    static List<String> listarCanais() {
        try {
            Map<String, Object> req = new HashMap<>();
            req.put("type", "LIST_CHANNELS_REQ");
            req.put("timestamp", System.currentTimeMillis());
            req.put("payload", new HashMap<>());

            reqSocket.send(mapper.writeValueAsBytes(req), 0);
            byte[] raw = reqSocket.recv(0);
            if (raw == null) return new ArrayList<>();

            Map<String, Object> resp = mapper.readValue(raw, Map.class);
            Map<String, Object> payload = (Map<String, Object>) resp.get("payload");
            List<String> canais = (List<String>) payload.get("channels");
            System.out.println("[" + botName + "] <-- LIST_CHANNELS_RESP | canais=" + canais);
            return canais != null ? canais : new ArrayList<>();
        } catch (Exception e) {
            System.err.println("[" + botName + "] Erro ao listar canais: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    static void criarCanal(String nome) {
        try {
            Map<String, Object> req = new HashMap<>();
            req.put("type", "CREATE_CHANNEL_REQ");
            req.put("timestamp", System.currentTimeMillis());
            req.put("payload", Map.of("channel_name", nome));

            System.out.println("[" + botName + "] --> CREATE_CHANNEL_REQ | canal=" + nome);
            reqSocket.send(mapper.writeValueAsBytes(req), 0);

            byte[] raw = reqSocket.recv(0);
            if (raw == null) return;

            Map<String, Object> resp = mapper.readValue(raw, Map.class);
            Map<String, Object> payload = (Map<String, Object>) resp.get("payload");
            System.out.println("[" + botName + "] <-- CREATE_CHANNEL_RESP | " + payload.get("message"));
        } catch (Exception e) {
            System.err.println("[" + botName + "] Erro ao criar canal: " + e.getMessage());
        }
    }

    static void publicarMensagem(String canal, String mensagem) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("channel", canal);
            payload.put("username", botName);
            payload.put("message", mensagem);

            Map<String, Object> req = new HashMap<>();
            req.put("type", "PUBLISH_REQ");
            req.put("timestamp", System.currentTimeMillis());
            req.put("payload", payload);

            System.out.println("[" + botName + "] --> PUBLISH_REQ | canal=" + canal + " | msg=" + mensagem);
            reqSocket.send(mapper.writeValueAsBytes(req), 0);

            byte[] raw = reqSocket.recv(0);
            if (raw == null) return;

            Map<String, Object> resp = mapper.readValue(raw, Map.class);
            Map<String, Object> respPayload = (Map<String, Object>) resp.get("payload");
            System.out.println("[" + botName + "] <-- PUBLISH_RESP | " + respPayload.get("message"));
        } catch (Exception e) {
            System.err.println("[" + botName + "] Erro ao publicar: " + e.getMessage());
        }
    }

    static ZMQ.Socket criarSocket() {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
        s.setReceiveTimeOut(5000);
        s.connect(brokerUrl);
        return s;
    }
}