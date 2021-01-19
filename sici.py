from zeep import Client
from tqdm import tqdm

from sici_site.models import Dados 

consumidor = ''
chaveAcesso = ''

client = Client('http://sici.rio.rj.gov.br/Servico/WebServiceSICI.asmx?wsdl')
retorno = client.service.Get_Arvore_UA(Codigo_UA='', Nivel='', Tipo_Arvore='', consumidor=consumidor, chaveAcesso=chaveAcesso)

codigos_ua = []
for item in retorno:
    for campo in item:
        if campo.tag == 'cd_ua' and campo.text != '' and campo.text != 'None':
            codigos_ua.append(campo.text) 

iterador = tqdm(codigos_ua)
for unidade in codigos_ua:
    iterador.set_description(unidade)
    
    detalhes = client.service.Get_Titular_Endereco_UA(
        chaveAcesso=chaveAcesso,
        consumidor=consumidor,
        Codigo_UA=unidade
    )

    detalhes_parseados = [[campo.text for campo in item] for item in detalhes]
    
    lista_valor = []
    for valor in detalhes_parseados[0]:
        if valor != None and valor != 'None' and valor != '':
            lista_valor.append(valor)
        else:
            lista_valor.append(None)

    if len(lista_valor) == 27 and lista_valor[0] != None:
        lista_final = Dados.objects.filter(cd_ua=lista_valor[0]).values_list(
                    'cd_ua', 'sigla_ua', 'nome_ua', 'titular', 'cargo' , 'cd_ua_pai', 'cd_ua_basica', 'nome_ua_basica',
                    'sigla_ua_basica', 'nat_juridica', 'ordem_ua_basica', 'ordem_absoluta', 'ordem_relativa', 'tipo_logradouro',
                    'nome_logradouro', 'trechamento_cep', 'nome_logradouro_abreviado', 'nro', 'complemento', 'bairro', 
                    'bairro_abreviado', 'localidade', 'cep', 'telefones', 'emails', 'horario_funcionamento', 'msg'
                    ).last()

        iguais = True
        if lista_final is None:
            iguais = False
            print(len(lista_valor), lista_valor)
            print('Lista Final = 0 27 - 1')

        else:
            for i in range(len(lista_final)):
                if str(lista_valor[i]) != str(lista_final[i]):
                    iguais = False
                    break
            print(len(lista_final), lista_final)
            print(len(lista_valor), lista_valor)


        if iguais:
            print('Lista Final = Lista String 27 - 2')
            
        else:
            print('Lista Final diferente da lista string 27 - 4')

            inserir = Dados(
                cd_ua=lista_valor[0],
                sigla_ua=lista_valor[1],
                nome_ua=lista_valor[2],
                titular=lista_valor[3],
                cargo=lista_valor[4],
                cd_ua_pai=lista_valor[5],
                cd_ua_basica=lista_valor[6],
                nome_ua_basica=lista_valor[7],
                sigla_ua_basica=lista_valor[8],
                nat_juridica=lista_valor[9],
                ordem_ua_basica=lista_valor[10],
                ordem_absoluta=lista_valor[11],
                ordem_relativa=lista_valor[12],
                tipo_logradouro=lista_valor[13],
                nome_logradouro=lista_valor[14],
                trechamento_cep=lista_valor[15],
                nome_logradouro_abreviado=lista_valor[16],
                nro=lista_valor[17],
                complemento=lista_valor[18],
                bairro=lista_valor[19],
                bairro_abreviado=lista_valor[20],
                localidade=lista_valor[21],
                cep=lista_valor[22],
                telefones=lista_valor[23],
                emails=lista_valor[24],
                horario_funcionamento=lista_valor[25],
                msg=lista_valor[26]
            )
            inserir.save()

    else:
        print("erro")
        with open('Log_erros.txt', 'a', encoding='utf-8') as f:
            f.write(str(lista_valor) + '\n')