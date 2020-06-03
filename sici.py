from zeep import Client
import os
import sqlite3
from tqdm import tqdm

lista = []

client = Client('http://sici.rio.rj.gov.br/Servico/WebServiceSICI.asmx?wsdl')

retorno = client.service.Get_Arvore_UA(Codigo_UA='', Nivel='', Tipo_Arvore='', consumidor='', chaveAcesso='')

arvore = [{campo.tag:campo.text for campo in item} for item in retorno]

iterador = tqdm(arvore)

for folha in tqdm(arvore):
    iterador.set_description(folha['nome_ua'])
    detalhes = client.service.Get_Titular_Endereco_UA(
        chaveAcesso='',
        consumidor='',
        Codigo_UA=folha['cd_ua']
    )
    detalhes_parseados = [{campo.tag:campo.text for campo in item} for item in detalhes]
    folha['titularidade'] = detalhes_parseados[0]
    for chave, valor in detalhes_parseados[0].items():
        lista.append(valor)


def create_send_db():
    os.remove('SICI.db') if os.path.exists('SICI.db') else None
    con = sqlite3.connect('SICI.db')
    cur = con.cursor()
    sql_create = 'create table Dados (id integer primary key, cd_ua integer, sigla_ua text, nome_ua text, titular text,'\
                 'cargo text, cd_ua_pai integer, cd_ua_basica text, nome_ua_basica text, sigla_ua_basica text,'\
                 'nat_juridica integer, ordem_ua_basica integer, ordem_absoluta integer, ordem_relativa integer,'\
                 'tipo_logradouro text, nome_logradouro text, trechamento_CEP integer, nome_logradouro_abreviado text,'\
                 'nro integer, complemento text, bairro text, bairro_abreviado text, localidade text, CEP integer,'\
                 'telefones text, emails text, horario_funcionamento text, msg text)'
    cur.execute(sql_create)

    cur.execute('INSERT INTO Dados (cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica, nome_ua_basica,'\
                'sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa, tipo_logradouro,'\
                'nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento, bairro, bairro_abreviado,'\
                'localidade, CEP, telefones, emails, horario_funcionamento, msg) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,'\
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (lista[0], lista[1], lista[2], lista[3], lista[4], lista[5],
                                                              lista[6], lista[7], lista[8], lista[9], lista[10], lista[11],
                                                              lista[12], lista[13], lista[14], lista[15], lista[16], lista[17],
                                                              lista[18], lista[19], lista[20], lista[21], lista[22], lista[23],
                                                              lista[24], lista[25], lista[26]))
    return con, cur


def close_db(con, cur):
    con.commit()
    cur.close()
    con.close()


def main():
    con, cur = create_send_db()

    close_db(con, cur)

if __name__ == "__main__":
    main()
