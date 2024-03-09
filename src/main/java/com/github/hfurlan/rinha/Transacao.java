package com.github.hfurlan.rinha;

public class Transacao {
    int valor;
    char tipo;
    String descricao;

    public Transacao(int valor, char tipo, String descricao) {
        this.valor = valor;
        this.tipo = tipo;
        this.descricao = descricao;
    }
}