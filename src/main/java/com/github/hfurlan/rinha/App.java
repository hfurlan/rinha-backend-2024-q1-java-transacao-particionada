package com.github.hfurlan.rinha;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class App {

    private String dbUsername;
    private String dbPassword;
    private String dbHostname;
    private String dbName;
    private int dbMaxConnections;
    private int httpPort;
    private int httpMaxThreads;
    private Map<Integer, Integer> clientesLimites = new ConcurrentHashMap<>();
    private Map<Integer, Integer> clientesSaldosIniciais = new ConcurrentHashMap<>();
    private Map<String, Connection> conns;

    public App() throws SQLException {
        dbUsername = System.getenv("DB_USERNAME");
        if (dbUsername == null) {
            dbUsername = "admin";
        }
        dbPassword = System.getenv("DB_PASSWORD");
        if (dbPassword == null) {
            dbPassword = "123";
        }
        dbHostname = System.getenv("DB_HOSTNAME");
        if (dbHostname == null) {
            dbHostname = "localhost";
        }
        dbName = System.getenv("DB_NAME");
        if (dbName == null) {
            dbName = "rinha";
        }
        String dbMaxConnectionsStr = System.getenv("DB_MAX_CONNECTIONS");
        if (dbMaxConnectionsStr == null) {
            dbMaxConnectionsStr = "10";
        }
        dbMaxConnections = Integer.parseInt(dbMaxConnectionsStr);
        System.out.println("Conectando no BD...");
        System.out.println("DB_HOSTNAME:" + dbHostname);
        System.out.println("DB_NAME:" + dbName);
        System.out.println("DB_USERNAME:" + dbUsername);
        System.out.println("DB_MAX_CONNECTIONS:" + dbMaxConnections);
        System.out.println("VERSAO:0.0.1-SNAPSHOT");

        String url = "jdbc:postgresql://"+dbHostname+"/"+dbName+"?ssl=false";
        conns = new HashMap<>();
        for (int i = 1; i <= dbMaxConnections; i++) {
            conns.put("pool-1-thread-" + i, DriverManager.getConnection(url, dbUsername, dbPassword));
        }
    }

    protected void startServer() throws IOException {
        String httpPortStr = System.getenv("HTTP_PORT");
        if (httpPortStr == null) {
            httpPortStr = "9999";
        }
        httpPort = Integer.parseInt(httpPortStr);

        String httpMaxThreadsStr = System.getenv("HTTP_MAX_THREADS");
        if (httpMaxThreadsStr == null) {
            httpMaxThreadsStr = "10";
        }
        httpMaxThreads = Integer.parseInt(httpMaxThreadsStr);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(httpMaxThreads);

        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/clientes", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String method = exchange.getRequestMethod();
                try {
                    Object[] serviceAndIdStr = HttpUtil.getServiceAndIdFromUri(exchange.getRequestURI());
                    String service = (String)serviceAndIdStr[0];
                    Integer id = (Integer)serviceAndIdStr[1];
                    if (service.equals("transacoes") && "POST".equals(method)) {
                        handleTransacoes(exchange, id);
                    } else if (service.equals("extrato") && "GET".equals(method)) {
                        handleExtrato(exchange, id);
                    } else {
                        responseError(exchange, 404, "Pagina nao encontrada");
                    }
                } catch (IllegalArgumentException e) {
                    responseError(exchange, 404, e.getMessage());
                } catch (Exception e) {
                    responseError(exchange, 500, e.getMessage());
                }
            }
        });
        server.setExecutor(threadPoolExecutor);
        server.start();
        System.out.println("Servidor iniciado e aguardando conexoes na PORTA " + httpPort + " (" + httpMaxThreads + " threads)");
    }

    protected void responseError(HttpExchange exchange, int httpErrorCode, String descricao) throws IOException {
        if (httpErrorCode == 500) {
            System.out.println("HTTP 500 - " + descricao);
        }
        OutputStream outputStream = exchange.getResponseBody();
        exchange.sendResponseHeaders(httpErrorCode, descricao.length());
        outputStream.write(descricao.getBytes());
        outputStream.flush();
        outputStream.close();
    }

    protected void handleTransacoes(HttpExchange exchange, int id) throws IOException, SQLException {
        Integer limite = obterClienteLimiteCache(id);
        if (limite == null) {
            responseError(exchange, 404, "Cliente nao encontrado");
            return;
        }

        byte[] request = exchange.getRequestBody().readAllBytes();
        Transacao transacao;
        try {
            transacao = HttpUtil.parseRequest(request);
        } catch (Exception e) {
            responseError(exchange, 422, e.getMessage());
            return;
        }
        if (transacao.valor <= 0) {
            responseError(exchange, 422, "Valor deve ser positivo");
            return;
        }
        if (transacao.tipo != 'c' && transacao.tipo != 'd') {
            responseError(exchange, 422, "Tipo deve ser 'c' ou 'd' (minusculo)");
            return;
        }

        if (transacao.descricao.length() < 1 || transacao.descricao.length() > 10) {
            responseError(exchange, 422, "Descricao deve ter tamanho entre 1 e 10");
            return;
        }

        boolean rsClosed = false;
        int saldo = -1;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = getConnection().prepareStatement("with novo_saldo as (UPDATE saldos SET saldo = saldo " + (transacao.tipo == 'c' ? '+' : '-') +" ? WHERE cliente_id = ? RETURNING saldo) insert into transacoes_" + id + " (valor, descricao, tipo, saldo) values (?, ?, ?, (select * from novo_saldo)) returning saldo");
            ps.setInt(1, transacao.valor);
            ps.setInt(2, id);
            ps.setInt(3, transacao.valor);
            ps.setString(4, transacao.descricao);
            ps.setString(5, "" + transacao.tipo);
            rs = ps.executeQuery();
            if (rs.next()) {
                saldo = rs.getInt(1);
            }
        } catch (Exception e) {
            closeQuiet(rs, ps);
            rsClosed = true;
            if (e.getMessage().contains("check")) {
                responseError(exchange, 422, "Sem saldo");
            } else if (e.getMessage().contains("not-null constraint")) {
                responseError(exchange, 404, "Cliente nao encontrado");
            } else {
                responseError(exchange, 500, e.getMessage());
            }
            return;
        } finally {
            if (!rsClosed) {
                closeQuiet(rs, ps);
            }
        }

        var json = new StringBuilder(100);
        json.append("{\"limite\":");
        json.append(limite);
        json.append(",\"saldo\":");
        json.append(saldo);
        json.append("}");
        OutputStream outputStream = exchange.getResponseBody();
        exchange.sendResponseHeaders(200, json.length());
        outputStream.write(json.toString().getBytes());
        outputStream.flush();
        outputStream.close();
    }

    protected void closeQuiet(ResultSet rs, PreparedStatement ps) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void closeQuiet(ResultSet rs, Statement st) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void handleExtrato(HttpExchange exchange, int id) throws IOException, SQLException {
        Integer limite = obterClienteLimiteCache(id);
        if (limite == null) {
            responseError(exchange, 404, "Cliente nao encontrado");
            return;
        }

        boolean escreveuLinhaAnterior = false;
        boolean escreveuSaldo = false;
        boolean rsClosed = false;
        var json = new StringBuilder(1200).append("{\"saldo\":{\"total\":");
        var dataExtratoStr = LocalDateTime.now().toString();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = getConnection().prepareStatement("select * from transacoes_" + id + " order by id desc limit 10");
            rs = ps.executeQuery();
            while (rs.next()) {
                if (!escreveuSaldo){
                    json.append(rs.getInt("saldo"))
                            .append(",\"data_extrato\":\"")
                            .append(dataExtratoStr)
                            .append("\",\"limite\":")
                            .append(limite)
                            .append("},\"ultimas_transacoes\":[");
                    escreveuSaldo = true;
                }
                if (escreveuLinhaAnterior) {
                    json.append(",");
                }
                json.append("{\"valor\":")
                    .append(rs.getInt("valor"))
                    .append(",\"tipo\":\"")
                    .append(rs.getString("tipo"))
                    .append("\",\"descricao\":\"")
                    .append(rs.getString("descricao"))
                    .append("\",\"realizada_em\":\"")
                    .append(rs.getString("data_hora_inclusao"))
                    .append("\"}");
                escreveuLinhaAnterior = true;
            }
            closeQuiet(rs, ps);
            rsClosed = true;
            if (!escreveuSaldo) { // nao encontrou nenhuma linha
                json.append(obterClienteSaldoInicialCache(id))
                        .append(",\"data_extrato\":\"")
                        .append(dataExtratoStr)
                        .append("\",\"limite\":")
                        .append(limite)
                        .append("},\"ultimas_transacoes\":[");
            }
            json.append("]}");
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(200, json.length());
            outputStream.write(json.toString().getBytes());
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            closeQuiet(rs, ps);
            responseError(exchange, 500, e.getMessage());
        } finally {
            if (!rsClosed) {
                closeQuiet(rs, ps);
            }
        }
    }

    private Connection getConnection() {
        return conns.get(Thread.currentThread().getName());
    }

    protected Integer obterClienteLimiteCache(int id) throws SQLException {
        Integer limite = clientesLimites.get(id);
        if (limite == null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = getConnection().prepareStatement("SELECT limite FROM clientes WHERE cliente_id = ?");
                ps.setInt(1, id);
                rs = ps.executeQuery();
                if (rs.next()) {
                    limite = rs.getInt(1);
                    clientesLimites.put(id, limite);
                } else {
                    return null;
                }
            } finally {
                closeQuiet(rs, ps);
            }
        }
        return limite;
    }

    protected Integer obterClienteSaldoInicialCache(int id) throws SQLException {
        Integer saldoInicial = clientesSaldosIniciais.get(id);
        if (saldoInicial == null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = getConnection().prepareStatement("SELECT saldo_inicial FROM clientes WHERE cliente_id = ?");
                ps.setInt(1, id);
                rs = ps.executeQuery();
                if (rs.next()) {
                    saldoInicial = rs.getInt(1);
                    clientesSaldosIniciais.put(id, saldoInicial);
                } else {
                    return null;
                }
            } finally {
                closeQuiet(rs, ps);
            }
        }
        return saldoInicial;
    }

    public static void main(String[] args) throws Exception {
        App app = new App();
        app.startServer();
    }
}
