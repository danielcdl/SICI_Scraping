import scrapy
from selenium import webdriver
import time

class MainSpider(scrapy.Spider):
    name = 'main-spider'
    start_urls = ['http://sici.rio.rj.gov.br/PAG/principal.aspx']

    def parse(self, response):

        ff = webdriver.Firefox()
        ff.get('http://sici.rio.rj.gov.br/PAG/principal.aspx')

        lista_arvore_direita = [response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblNomeUnidadeGestaoSelecionada"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblSiglaUnidadeGestaoSelecionada"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblCodigoUnidadeGestaoSelecionada"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloTitular"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblTitular"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloCargo"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblCargo"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloEndereco"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblEndereco"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloNumero"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblNumero"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloComplemento"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblComplemento"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloBairro"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblBairro"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblRotuloCEP"]/text()').extract(),
                                response.xpath('//div//div//span[@id="ContentPlaceHolder1_lblCEP"]/text()').extract()
                                ]

        ff.find_element_by_id("ContentPlaceHolder1_ua_treeviewt0").click()
        for xpath in lista_arvore_direita:
            print(xpath)
        ff.find_element_by_id("ContentPlaceHolder1_ua_treeviewt1").click()
        time.sleep(2)
        ff.get('http://sici.rio.rj.gov.br/PAG/principal.aspx')
        time.sleep(2)
        for xpath in lista_arvore_direita:
            print(xpath)
        ff.find_element_by_id("ContentPlaceHolder1_ua_treeviewt2").click()
        for xpath in lista_arvore_direita:
            return xpath

