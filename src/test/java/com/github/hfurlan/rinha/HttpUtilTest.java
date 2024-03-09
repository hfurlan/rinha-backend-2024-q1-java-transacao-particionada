package com.github.hfurlan.rinha;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class HttpUtilTest {

    @Test
    public void parseRequest_payloadValido_retornaTransacao() {
        String json = "{\"valor\": 1000, \"tipo\" : \"c\", \"descricao\" : \"teste\"}";
        Transacao transacao = HttpUtil.parseRequest(json.getBytes());
        assertEquals( 1000, transacao.valor);
        assertEquals( 'c', transacao.tipo);
        assertEquals( "teste", transacao.descricao);
    }


    @Test(expected = IllegalArgumentException.class)
    public void parseRequest_descricaoSemAspas_retornaException() {
        String json = "{\"valor\": 1000, \"tipo\" : \"c\", \"descricao\" : teste}";
        HttpUtil.parseRequest(json.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRequest_tipoSemAspas_retornaException() {
        String json = "{\"valor\": 1000, \"tipo\" : c, \"descricao\" : \"teste\"}";
        HttpUtil.parseRequest(json.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRequest_valorComAspas_retornaException() {
        String json = "{\"valor\": \"1000\", \"tipo\" : \"c\", \"descricao\" : \"teste\"}";
        HttpUtil.parseRequest(json.getBytes());
    }

    @Test
    public void getServiceAndIdFromUri_pathValido_retornaServicoEId() throws URISyntaxException {
        Object[] valores = HttpUtil.getServiceAndIdFromUri(new URI("http://localhost:9999/clientes/1/transacoes"));
        assertEquals( "transacoes", valores[0]);
        assertEquals( 1, valores[1]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getServiceAndIdFromUri_pathInvalido_retornaException() throws URISyntaxException {
        String path = "/clientes/1/transacoes";
        HttpUtil.getServiceAndIdFromUri(new URI("http://localhost:9999/clientes/transacoes"));
    }

}