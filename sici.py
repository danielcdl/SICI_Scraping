from zeep import Client
import os
import sqlite3
from tqdm import tqdm

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
    for i in detalhes:
        for campo2 in i:
            print(campo2.text.split)
#    folha['titularidade'] = detalhes_parseados[0]
    break


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

    sql_envia_dados = ('INSERT INTO Dados (cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica, nome_ua_basica,'
                'sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa, tipo_logradouro, '
                'nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento, bairro, bairro_abreviado,'
                'localidade, CEP, telefones, emails, horario_funcionamento, msg) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (detalhes_parseados[0], detalhes_parseados[1],
                                                                    detalhes_parseados[2], detalhes_parseados[3],
                                                                    detalhes_parseados[4], detalhes_parseados[5],
                                                                    detalhes_parseados[6], detalhes_parseados[7],
                                                                    detalhes_parseados[8], detalhes_parseados[9],
                                                                    detalhes_parseados[10], detalhes_parseados[11],
                                                                    detalhes_parseados[12], detalhes_parseados[13],
                                                                    detalhes_parseados[14], detalhes_parseados[15],
                                                                    detalhes_parseados[16], detalhes_parseados[17],
                                                                    detalhes_parseados[18], detalhes_parseados[19],
                                                                    detalhes_parseados[20], detalhes_parseados[21],
                                                                    detalhes_parseados[22], detalhes_parseados[23],
                                                                    detalhes_parseados[24], detalhes_parseados[25],
                                                                    detalhes_parseados[26]))


    cur.execute(sql_envia_dados)

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
