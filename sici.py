from zeep import Client
import os
import sqlite3
from tqdm import tqdm
import pdb

os.remove('SICI3.db') if os.path.exists('SICI3.db') else None
con = sqlite3.connect('SICI3.db')
cur = con.cursor()
sql_create = 'create table Dados (id integer primary key, cd_ua integer, sigla_ua text, nome_ua text, titular text,'\
             'cargo text, cd_ua_pai integer, cd_ua_basica text, nome_ua_basica text, sigla_ua_basica text,'\
             'nat_juridica integer, ordem_ua_basica integer, ordem_absoluta integer, ordem_relativa integer,'\
             'tipo_logradouro text, nome_logradouro text, trechamento_CEP integer, nome_logradouro_abreviado text,'\
             'nro integer, complemento text, bairro text, bairro_abreviado text, localidade text, CEP integer,'\
             'telefones text, emails text, horario_funcionamento text, msg text)'

cur.execute(sql_create)

def close_db(con, cur):
    con.commit()
    cur.close()
    con.close()

counter = 1

lista_chave = []
lista_valor = []
lista_diferente_27 = []

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
        lista_chave.append(chave)
        lista_valor.append(valor)

    if len(lista_valor) == 27:
        cur.execute('INSERT INTO Dados (cd_ua, sigla_ua, nome_ua, titular, cargo , cd_ua_pai, cd_ua_basica,'
                    ' nome_ua_basica, sigla_ua_basica, nat_juridica, ordem_ua_basica, ordem_absoluta, ordem_relativa,'
                    ' tipo_logradouro, nome_logradouro, trechamento_CEP, nome_logradouro_abreviado, nro, complemento,'
                    ' bairro, bairro_abreviado, localidade, CEP, telefones, emails, horario_funcionamento, msg)'
                    ' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (lista_valor[0], lista_valor[1], lista_valor[2], lista_valor[3], lista_valor[4], lista_valor[5],
                     lista_valor[6], lista_valor[7], lista_valor[8], lista_valor[9], lista_valor[10], lista_valor[11],
                     lista_valor[12], lista_valor[13], lista_valor[14], lista_valor[15], lista_valor[16], lista_valor[17],
                     lista_valor[18], lista_valor[19], lista_valor[20], lista_valor[21], lista_valor[22], lista_valor[23],
                     lista_valor[24], lista_valor[25], lista_valor[26]))
        con.commit()
        lista_valor.clear()
    else:
        if len(lista_valor) < 27 or len(lista_valor) > 27:
            with open('Log_erros.txt', 'a', encoding='utf-8') as f:
                f.write(str(lista_valor) + '\n')
            lista_valor.clear()
        continue
#    except:
#        pass

close_db(con, cur)
