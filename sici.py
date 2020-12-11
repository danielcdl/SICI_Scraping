from zeep import Client
import os
import sqlite3
from tqdm import tqdm
from datetime import datetime

os.remove('SICI.db') if os.path.exists('SICI.db') else None
con = sqlite3.connect('SICI.db')
cur = con.cursor()
con.commit()

sql_create = 'create table Dados (id integer IDENTITY(1,1) primary key, cd_ua integer, sigla_ua nvarchar(100), nome_ua nvarchar(255), titular nvarchar(255),'\
           'cargo nvarchar(255), cd_ua_pai integer, cd_ua_basica integer, nome_ua_basica nvarchar(255), sigla_ua_basica nvarchar(255),'\
           'nat_juridica integer, ordem_ua_basica integer, ordem_absoluta integer, ordem_relativa integer,'\
           'tipo_logradouro nvarchar(255), nome_logradouro nvarchar(500), trechamento_CEP nvarchar(500), nome_logradouro_abreviado nvarchar(500),'\
           'nro integer, complemento nvarchar(255), bairro nvarchar(255), bairro_abreviado nvarchar(255), localidade nvarchar(255), CEP nvarchar(255),'\
           'telefones nvarchar(1000), emails nvarchar(1000), horario_funcionamento nvarchar(255), msg text, data_criacao_registro datetime)'

cur.execute(sql_create)
con.commit()

def close_db(con, cur):
    con.commit()
    cur.close()
    con.close()

lista_chave = []
lista_valor = []
lista_final = []

client = Client('http://sici.rio.rj.gov.br/Servico/WebServiceSICI.asmx?wsdl')

retorno = client.service.Get_Arvore_UA(Codigo_UA='', Nivel='', Tipo_Arvore='', consumidor='', chaveAcesso='')

arvore = [{campo.tag: campo.text for campo in item} for item in retorno]

iterador = tqdm(arvore)

for folha in arvore:
    iterador.set_description(folha['nome_ua'])
    detalhes = client.service.Get_Titular_Endereco_UA(
        chaveAcesso='',
        consumidor='',
        Codigo_UA=folha['cd_ua']
    )
    detalhes_parseados = [{campo.tag:campo.text for campo in item} for item in detalhes]
    folha['titularidade'] = detalhes_parseados[0]

    for chave, valor in detalhes_parseados[0].items():
        lista_chave.append(chave)
        if valor != None:
            lista_valor.append(valor)
        else:
            lista_valor.append('')

    if len(lista_valor) == 27 and lista_valor[0] != '':
        print(len(lista_valor), lista_valor)
        selecao_cd_ua = cur.execute('SELECT cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica,'
                            ' nome_ua_basica, sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa,'
                            ' tipo_logradouro, nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento,'
                            ' bairro, bairro_abreviado, localidade, CEP, telefones, emails, horario_funcionamento, msg FROM Dados WHERE cd_ua=' + lista_valor[int(0)])
        for x in selecao_cd_ua:
            if x != None:
                lista_final = [str(y) if y != None else str(y) for y in x]

        if len(lista_final) == 0:
            print(len(lista_final), lista_final)
            print(len(lista_valor), lista_valor)
            print('Lista Final = 0 27 - 1')
            cur.execute('INSERT INTO Dados (cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica,'
                        ' nome_ua_basica, sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa,'
                        ' tipo_logradouro, nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento,'
                        ' bairro, bairro_abreviado, localidade, CEP, telefones, emails, horario_funcionamento, msg, data_criacao_registro)'
                        ' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (lista_valor[0], lista_valor[1], lista_valor[2], lista_valor[3], lista_valor[4],
                         lista_valor[5],
                         lista_valor[6], lista_valor[7], lista_valor[8], lista_valor[9], lista_valor[10],
                         lista_valor[11],
                         lista_valor[12], lista_valor[13], lista_valor[14], lista_valor[15], lista_valor[16],
                         lista_valor[17],
                         lista_valor[18], lista_valor[19], lista_valor[20], lista_valor[21], lista_valor[22],
                         lista_valor[23],
                         lista_valor[24], lista_valor[25], lista_valor[26], datetime.now()))
            con.commit()
            lista_valor.clear()
            lista_final.clear()

        elif lista_valor == lista_final:
            print(len(lista_final), lista_final)
            print(len(lista_valor), lista_valor)
            print('Lista Final = Lista String 27 - 2')
            lista_valor.clear()
            lista_final.clear()
        else:

            if lista_final[17] == '0' or lista_final[17] == 0 and lista_valor[17] == '':
                print('Lista Final diferente da lista string 27 - 3')
                print(len(lista_final), lista_final)
                print(len(lista_valor), lista_valor)
                lista_valor.clear()
                lista_final.clear()

            else:
                print(len(lista_final), lista_final)
                print(len(lista_valor), lista_valor)
                print('Lista Final diferente da lista string 27 - 4')
                cur.execute('INSERT INTO Dados (cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica,'
                            ' nome_ua_basica, sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa,'
                            ' tipo_logradouro, nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento,'
                            ' bairro, bairro_abreviado, localidade, CEP, telefones, emails, horario_funcionamento, msg, data_criacao_registro)'
                            ' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            (lista_valor[0], lista_valor[1], lista_valor[2], lista_valor[3], lista_valor[4],
                             lista_valor[5],
                             lista_valor[6], lista_valor[7], lista_valor[8], lista_valor[9], lista_valor[10],
                             lista_valor[11],
                             lista_valor[12], lista_valor[13], lista_valor[14], lista_valor[15], lista_valor[16],
                             lista_valor[17],
                             lista_valor[18], lista_valor[19], lista_valor[20], lista_valor[21], lista_valor[22],
                             lista_valor[23],
                             lista_valor[24], lista_valor[25], lista_valor[26], datetime.now()))
                con.commit()
                lista_valor.clear()
                lista_final.clear()

    elif lista_valor[0] == '':
        lista_valor.clear()
        lista_final.clear()

    elif len(lista_valor) < 27 or len(lista_valor) > 27:
        with open('Log_erros.txt', 'a', encoding='utf-8') as f:
            f.write(str(lista_valor) + '\n')
        lista_valor.clear()

    continue

close_db(con, cur)
