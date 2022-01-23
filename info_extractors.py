import re
from copy import deepcopy
from datetime import datetime


class FirstInfoExtractor:
    '''
    Purpose: Extracts all relevant information for File 1 (Information on products and services sold) from ONE txt file

    Initial parameters:
        1. Path to text file
        2. List of dictionaries which will form the final dataframe
    '''

    def __init__(self, path, df):
        self.filepath = path
        self.trunc_filepath = self.filepath.removeprefix('bids/txt/')
        self.df1 = df
        self.col_dict = {'FileName': self.trunc_filepath}
        self.within_row = False
        self.stop_count = -1

        with open(path, encoding="ISO-8859-1") as f:
            self.lines = f.readlines()

    def update_df(self):
        return self.df1

    @staticmethod
    def line_split_to_dict(input_line, in_dict, dict_head=None, cleanse=False, split_on=': '):
        tmp_line = input_line
        if cleanse:
            tmp_line = tmp_line.removesuffix('"\n')
            tmp_line = tmp_line.removeprefix('"')
        ht_pair = re.split(split_on, tmp_line, maxsplit=1)
        out_dict = deepcopy(in_dict)
        if dict_head is not None:
            out_dict[dict_head] = ht_pair[1]
        else:
            out_dict[ht_pair[0]] = ht_pair[1]
        return out_dict

    def extract_and_add(self):
        first_file_cond = True
        row_num = 0
        for line in self.lines:
            row_num += 1
            self.col_dict['FileRow'] = row_num
            if re.fullmatch('"Histórico"\n', line) is not None and first_file_cond:
                first_file_cond = False
            if line.count(': ') > 1 and first_file_cond:
                if (self.within_row or self.stop_count >= 0) and re.fullmatch('^"Aceito para: .* (.*: .*) \."\n',
                                                                              line) is not None:
                    self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                    self.within_row = False

                    self.df1.append(self.col_dict)

                    self.col_dict = {'FileName': self.trunc_filepath}

                elif (self.within_row or self.stop_count >= 0) and re.fullmatch(
                        '^"Descrição Complementar: .* (.*: .*) .*"\n$',
                        line) is not None:
                    self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)

                else:
                    if (self.within_row or self.stop_count >= 0) and re.fullmatch(
                            '^"Quantidade: .*[\s]*Unidade de fornecimento: .*"\n$', line) is not None:
                        line = line.removesuffix('"\n')
                        line = line.removeprefix('"')
                        temp_line = line.split(' Unidade de fornecimento: ')
                        temp_line1 = temp_line[0]
                        temp_line2 = 'Unidade de fornecimento: ' + temp_line[1]
                        self.col_dict = self.line_split_to_dict(temp_line1, self.col_dict)
                        self.col_dict = self.line_split_to_dict(temp_line2, self.col_dict)

                    elif (self.within_row or self.stop_count >= 0) and re.fullmatch(
                            '^"Valor [Ee]stimado: .*[\s]*Situação: .*"\n$', line) is not None:
                        line = line.removesuffix('"\n')
                        line = line.removeprefix('"')
                        temp_line = line.split(' Situação: ')
                        temp_line1 = temp_line[0]
                        temp_line2 = 'Situação: ' + temp_line[1]
                        self.col_dict = self.line_split_to_dict(temp_line1, self.col_dict, dict_head='Valor Estimado')
                        self.col_dict = self.line_split_to_dict(temp_line2, self.col_dict)
                        if re.fullmatch(
                                '.* Situação: Cancelado na aceitação$|.* Situação: Cancelado por inexistência de proposta$|.* Situação: Em análise$',
                                line) is not None:
                            # End early
                            self.within_row = False
                            self.df1.append(self.col_dict)
                            self.col_dict = {'FileName': self.trunc_filepath}
                        elif re.fullmatch('.* Situação: Cancelado no julgamento$', line) is not None:
                            self.within_row = False
                            self.stop_count = 2
                    elif (self.within_row or self.stop_count >= 0) and re.fullmatch(
                            '^"Aplicabilidade Decreto 7174: .*[\s]*Aplicabilidade Margem de Preferência: .*"\n$',
                            line) is not None:
                        line = line.removesuffix('"\n')
                        line = line.removeprefix('"')
                        temp_line = line.split(' Aplicabilidade Margem de Preferência: ')
                        temp_line1 = temp_line[0]
                        temp_line2 = 'Aplicabilidade Margem de Preferência: ' + temp_line[1]
                        self.col_dict = self.line_split_to_dict(temp_line1, self.col_dict,
                                                                dict_head='Aplicabilidade Decreto 7174')
                        self.col_dict = self.line_split_to_dict(temp_line2, self.col_dict,
                                                                dict_head='Aplicabilidade Margem de Preferência')
            elif line.count(': ') == 1 and first_file_cond:
                if re.fullmatch('^"Item: [0-9][0-9]*"\n$|^"Item: [0-9][0-9]* - .*"\n', line) is not None and not self.within_row:
                    self.within_row = True
                    #self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                    line = line.removeprefix('"')
                    line = line.removesuffix('"\n')
                    self.col_dict['Item Name'] = re.findall('Item: [0-9,.]*.*$', line)[0]
                elif (self.within_row or self.stop_count >= 0) and (
                        re.fullmatch('^"Aceito para: .* \."\n$', line) is not None or
                        re.fullmatch('^"Adjudicado para: .*"\n$', line) is not None):
                    self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                    self.within_row = False
                    self.df1.append(self.col_dict)
                    self.col_dict = {'FileName': self.trunc_filepath}
                elif self.within_row or self.stop_count >= 0:
                    if re.fullmatch('""\n|^"Descrição Complementar:"\n$', line) is not None:
                        continue
                    else:
                        self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                        if self.stop_count == 0:
                            self.df1.append(self.col_dict)
                            self.col_dict = {'FileName': self.trunc_filepath}
            self.stop_count -= 1


class MidInfoExtractor:

    def __init__(self, path, df2, df3):
        self.filepath = path
        self.trunc_filepath = self.filepath.removeprefix('bids/txt/')

        self.within_sect = False
        self.within_two = False
        self.within_three = False
        self.current_item = ""

        self.df2 = df2
        self.df3 = df3

        self.col_dict2 = {}
        self.col_dict3 = {}

        with open(path, encoding="ISO-8859-1") as f:
            self.lines = f.readlines()

    def update_second_df(self):
        return self.df2

    def update_third_df(self):
        return self.df3

    def second_parser(self, input_line, line_type, line_row, ast_bool=False, cleanse=False):
        # Returns a dictionary with a single line's information parsed
        temp_line = input_line
        if cleanse:
            temp_line = temp_line.removesuffix('"\n')
            temp_line = temp_line.removeprefix('"')

        if line_type == '1a' or line_type == '2a':
            uno_dict = {'FileName': self.trunc_filepath,
                        'FileRow': line_row,
                        'Item Name': self.current_item,
                        'CNPJ/CPF': re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]}
            res_str = ('(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Não .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Não .*)')
            uno_dict['Fornecedor'] = re.findall(res_str, temp_line)[0]
            list_one = re.findall('Sim Sim|Sim Não|Não Sim|Não Não', temp_line)
            list_two = re.split(' ', list_one[0])
            if line_type == '1a':
                uno_dict['Porte ME/EPP'] = list_two[0]
                uno_dict['Declaração ME/EPP/COOP'] = list_two[1]
            elif line_type == '2a':
                uno_dict['ME/EPP Equiparada'] = list_two[0]
                uno_dict['Declaração ME/EPP'] = list_two[1]
            res_str = None

            res_str = ('(?<=Sim Sim ).*(?= R\$.*R\$)|'
                       '(?<=Sim Não ).*(?= R\$.*R\$)|'
                       '(?<=Não Sim ).*(?= R\$.*R\$)|'
                       '(?<=Não Não ).*(?= R\$.*R\$)')
            uno_dict['Quantidade'] = re.findall(res_str, temp_line)[0]
            valor_line = re.findall(' R\$ [0-9][^\s]*[0-9] R\$ [0-9][^\s]*', temp_line)[0]
            valor_line2 = re.split(' R\$ ', valor_line)
            uno_dict['Valor Unit. (R$)'] = valor_line2[1]
            uno_dict['Valor Global (R$)'] = valor_line2[2]
            uno_time = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            uno_dict['Data/Hora Registro'] = datetime.strptime(uno_time, '%d/%m/%Y %H:%M:%S')
            uno_dict['Asterisk'] = 1 if ast_bool else 0
            del list_one, list_two, valor_line, valor_line2
            return uno_dict

        elif line_type == '3a':
            tres_dict = {'FileName': self.trunc_filepath,
                        'FileRow': line_row,
                        'Item Name': self.current_item,
                        'CNPJ/CPF': re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]}
            res_str = '(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= [0-9,.]* R\$ [0-9,.]* R\$ .*)'
            tres_dict['Fornecedor'] = re.findall(res_str, temp_line)[0]
            res_str = None

            res_str = '(?<=\s)[0-9,.]*(?= R\$ [0-9,.]* R\$ .*)'
            tres_dict['Quantidade'] = re.findall(res_str, temp_line)[0]
            valor_line = re.findall(' R\$ [0-9][^\s]*[0-9] R\$ [0-9][^\s]*', temp_line)[0]
            valor_line2 = re.split(' R\$ ', valor_line)
            tres_dict['Valor Unit. (R$)'] = valor_line2[1]
            tres_dict['Valor Global (R$)'] = valor_line2[2]
            tres_time = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            tres_dict['Data/Hora Registro'] = datetime.strptime(tres_time, '%d/%m/%Y %H:%M:%S')
            tres_dict['Asterisk'] = 1 if ast_bool else 0
            del valor_line, valor_line2
            return tres_dict

        elif line_type == '4b':
            quad_dict = {'FileName': self.trunc_filepath,
                         'FileRow': line_row,
                         'Item Name': self.current_item,
                         'CNPJ/CPF': re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]}
            res_str = ('(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Não .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Não .*)')
            quad_dict['Fornecedor'] = re.findall(res_str, temp_line)[0]
            list_one = re.findall('Sim Sim|Sim Não|Não Sim|Não Não', temp_line)
            list_two = re.split(' ', list_one[0])
            quad_dict['Porte ME/EPP'] = list_two[0]
            quad_dict['Declaração ME/EPP/COOP'] = list_two[1]
            res_str = None

            res_str = ('(?<=Sim Sim ).*(?= [0-9,.]* %)|'
                       '(?<=Sim Não ).*(?= [0-9,.]* %)|'
                       '(?<=Não Sim ).*(?= [0-9,.]* %)|'
                       '(?<=Não Não ).*(?= [0-9,.]* %)')
            quad_dict['Quantitade'] = re.findall(res_str, temp_line)[0]
            res_str = None

            res_str = ('(?<=Sim Sim ).*(?= R\$)|'
                       '(?<=Sim Não ).*(?= R\$)|'
                       '(?<=Não Sim ).*(?= R\$)|'
                       '(?<=Não Não ).*(?= R\$)')
            desconto_line = re.findall(res_str, temp_line)[0]
            quad_dict['Desconto'] = desconto_line.removeprefix(quad_dict['Quantitade'] + ' ')

            quad_dict['Valor com Desconto (R$)'] = re.findall('(?<=% R\$ ).*(?= [0-9]{2}/[0-9]{2}/[0-9]{4})', temp_line)[0]
            time_line = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            quad_dict['Data/Hora Registsro'] = datetime.strptime(time_line, '%d/%m/%Y %H:%M:%S')
            quad_dict['Asterisk'] = 1 if ast_bool else 0
            del desconto_line, list_one, list_two, time_line
            return quad_dict

        elif line_type == '5':
            cinco_dict = {'FileName': self.trunc_filepath,
                          'FileRow': line_row,
                          'Item Name': self.current_item,
                          'CNPJ/CPF': re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]}
            res_str = ('(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Sim Não .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Sim .*)'
                       '|(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*(?= Não Não .*)')
            cinco_dict['Fornecedor'] = re.findall(res_str, temp_line)[0]
            list_one = re.findall('Sim Sim|Sim Não|Não Sim|Não Não', temp_line)
            list_two = re.split(' ', list_one[0])
            cinco_dict['Porte ME/EPP'] = list_two[0]
            cinco_dict['Declaração ME/EPP/COOP'] = list_two[1]
            res_str = None

            res_str = ('(?<=Sim Sim ).*(?= [0-9,.]* R\$ [0-9,.]* R\$)|'
                       '(?<=Sim Não ).*(?= [0-9,.]* R\$ [0-9,.]* R\$)|'
                       '(?<=Não Sim ).*(?= [0-9,.]* R\$ [0-9,.]* R\$)|'
                       '(?<=Não Não ).*(?= [0-9,.]* R\$ [0-9,.]* R\$)')
            cinco_dict['Declaração PPB/TP'] = re.findall(res_str, temp_line)[0]
            res_str = None

            res_str = ('(?<=Sim Sim ).*(?= R\$ [0-9,.]* R\$)|'
                       '(?<=Sim Não ).*(?= R\$ [0-9,.]* R\$)|'
                       '(?<=Não Sim ).*(?= R\$ [0-9,.]* R\$)|'
                       '(?<=Não Não ).*(?= R\$ [0-9,.]* R\$)')
            qtd_hold = re.findall(res_str, temp_line)[0]
            cinco_dict['Quantidade'] = qtd_hold.removeprefix(cinco_dict['Declaração PPB/TP'] + ' ')

            valor_line = re.findall(' R\$ [0-9][^\s]*[0-9] R\$ [0-9][^\s]*', temp_line)[0]
            valor_line2 = re.split(' R\$ ', valor_line)
            cinco_dict['Valor Unit. (R$)'] = valor_line2[1]
            cinco_dict['Valor Global (R$)'] = valor_line2[2]
            time_line = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            cinco_dict['Data/Hora Registro'] = datetime.strptime(time_line, '%d/%m/%Y %H:%M:%S')
            cinco_dict['Asterisk'] = 1 if ast_bool else 0
            del time_line, list_one, list_two, qtd_hold

            return cinco_dict

        elif line_type == '6':
            seis_dict = {'FileName': self.trunc_filepath,
                         'FileRow': line_row,
                         'Item Name': self.current_item,
                         'CNPJ/CPF': re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]}
            res_str = ('(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*'
                       '(?= [0-9,.]* [0-9,.]* [0-9,.]* .* [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2})')
            seis_dict['Fornecedor'] = re.findall(res_str, temp_line)[0]
            res_str = None

            res_str = ('(?<=[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} ).*'
                       '(?= [0-9,.]* [0-9,.]* .* [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2})')
            qtde_line = re.findall(res_str, temp_line)[0]
            seis_dict['Quantitade'] = qtde_line.removeprefix(seis_dict['Fornecedor'] + ' ')
            res_str = None

            res_str = '(?<= {0} {1} )[0-9,.]* [0-9,.]*(?= .* [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9])'.format(
                seis_dict['Fornecedor'], seis_dict['Quantitade']
            )
            valor_line1 = re.findall(res_str, temp_line)[0]
            valor_line2 = re.split(' ', valor_line1, maxsplit=1)
            seis_dict['Valor Unit. (R$)'] = valor_line2[0]
            seis_dict['Valor Global (R$)'] = valor_line2[1]
            res_str = None

            res_str = '(?<={0} {1} ).*(?= [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9])'.format(
                seis_dict['Valor Unit. (R$)'], seis_dict['Valor Global (R$)']
            )
            seis_dict['Marca'] = re.findall(res_str, temp_line)[0]
            time_line = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            seis_dict['Data/Hora Registro'] = datetime.strptime(time_line, '%d/%m/%Y %H:%M:%S')
            seis_dict['Asterisk'] = 1 if ast_bool else 0
            return seis_dict

    def third_parser(self, input_line, line_type, line_row, ast_bool=False, cleanse=False):
        temp_line = input_line
        if cleanse:
            temp_line = temp_line.removesuffix('"\n')
            temp_line = temp_line.removeprefix('"')
        tmp_dict = {'FileName': self.trunc_filepath, 'FileRow': line_row}
        if re.fullmatch('\* .*', temp_line) is not None:
            ast_bool = True
        if line_type in ['1a', '2a', '3a', '5', '6']:
            tmp_dict['Item Name'] = self.current_item
            tmp_dict['CNPJ/CPF'] = re.findall('[0-9]{2}[,.][0-9]{3}[,.][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]
            tmp_list = re.split(' [0-9]*\.[0-9]*\.[0-9]*/[0-9]*-[0-9]* ', temp_line)
            tmp_dict['Valor do Lance (R$)'] = tmp_list[0].removeprefix('R$ ')
            tmp_time = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', tmp_list[1])[0]
            tmp_dict['Data/Hora Registro'] = datetime.strptime(tmp_time, '%d/%m/%Y %H:%M:%S')
            tmp_dict['Asterisk'] = 1 if ast_bool else 0
        elif line_type == '4b':
            tmp_dict['Item Name'] = self.current_item
            tmp_dict['Desconto'] = re.findall('.* %(?= R\$)', temp_line)[0]
            tmp_dict['Valor com Desconto (R$)'] = re.findall('(?<=% R\$ ).*(?= [0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/)', temp_line)[0]
            tmp_dict['CNPJ/CPF'] = re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]
            time_sto = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            tmp_dict['Data/Hora Registro'] = datetime.strptime(time_sto, '%d/%m/%Y %H:%M:%S')
            tmp_dict['Asterisk'] = 1 if ast_bool else 0
        elif line_type == '7':
            tmp_dict['Item Name'] = self.current_item
            lance_line = re.findall('(?<=R\$ )[0-9,.]*(?=\s)', temp_line)
            tmp_dict['Valor do Lance (R$)'] = lance_line[0]
            fator_str = '(?<={0} )[0-9,.]*(?= R\$)'.format(lance_line[0])
            tmp_dict['Fator de Equalização'] = re.findall(fator_str, temp_line)[0]
            tmp_dict['Valor do Lance Equalizado R($)'] = lance_line[1]
            tmp_dict['CNPJ/CPF'] = re.findall('[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2}', temp_line)[0]
            time_sto = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', temp_line)[0]
            tmp_dict['Data/Hora Registro'] = datetime.strptime(time_sto, '%d/%m/%Y %H:%M:%S')
            tmp_dict['Asterisk'] = 1 if ast_bool else 0

        return tmp_dict

    def extract_and_add(self):
        row_num = 0
        tmp_param = ''
        third_temp_param = ''
        for line in self.lines:
            row_num += 1
            if not self.within_sect and re.fullmatch('"Histórico"\n', line) is not None:
                # Start of section
                self.within_sect = True
            elif self.within_sect and not self.within_three and not self.within_two and re.fullmatch(
                    '"[\s]*Troca de Mensagens"\n', line) is not None:
                # End of section
                self.within_sect = False
            elif self.within_sect and not self.within_two and not self.within_three:
                if re.fullmatch('^"[\s]*Item: [0-9][0-9]* - .*"\n$', line) is not None:
                    self.within_two = True
                    line = line.removesuffix('"\n')
                    line = line.removeprefix('"')
                    self.current_item = re.findall('Item: [0-9,.]*.*$', line)[0]

            elif self.within_sect and self.within_two and not self.within_three:
                if re.fullmatch('"Lances (.*)"\n', line) is not None:
                    self.df2.append(self.col_dict2)
                    self.within_two = False
                    self.within_three = True
                elif re.fullmatch('"ME/EPP/COOP Quantidade Valor Unit. Valor Global Data/Hora Registro"\n', line) is not None:
                    # For Types 1a and 1b
                    tmp_param = '1a'
                elif re.fullmatch('"ME/EPP Quantidade Valor Unit. Valor Global Data/Hora Registro"\n', line) is not None:
                    # For Types 2a and 2b
                    tmp_param = '2a'
                elif re.fullmatch('"CNPJ/CPF Fornecedor Quantidade Valor Unit. Valor Global Data/Hora Registro"\n', line) is not None:
                    # For Type 3a
                    tmp_param = '3a'
                elif re.fullmatch('"ME/EPP/COOP Quantidade Desconto Valor com Desconto Data/Hora Registro"\n', line) is not None:
                    # For Type 4b
                    tmp_param = '4b'
                elif re.fullmatch('"PPB/TP Quantidade Valor Unit. Valor Global Data/Hora Registro"\n', line) is not None:
                    # For Type 5
                    tmp_param = '5'
                elif re.fullmatch('"CNPJ/CPF Fornecedor Qtde Valor Unit. \(R\$\) Valor Global \(R\$\) Marca Data/Hora Registro"\n', line) is not None:
                    # For Type 6
                    tmp_param = '6'

                elif re.fullmatch('^"[0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} .* [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}"\n$',
                                  line) is not None:
                    if self.col_dict2:
                        self.df2.append(self.col_dict2)
                    self.col_dict2 = self.second_parser(line, tmp_param, row_num, cleanse=True) # Differentiating between types done in second_parser function

                elif re.fullmatch('^"\* [0-9]{2}[.,][0-9]{3}[.,][0-9]{3}/[0-9]{4}-[0-9]{2} .* [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}"\n$',
                                  line) is not None: # For disqualified proposals
                    if self.col_dict2:
                        self.df2.append(self.col_dict2)
                    undq_line = line.removeprefix('"* ')
                    undq_line = undq_line.removesuffix('"\n')
                    self.col_dict2 = self.second_parser(undq_line, tmp_param, row_num, ast_bool=True, cleanse=False)

                elif re.fullmatch('^"Marca: .*"\n$', line) is not None: #Marca
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Marca'] = re.split(': ', proc_line, maxsplit=1)[1]
                elif re.fullmatch('^"Fabricante: .*"\n$', line) is not None: #Fabricante
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Fabricante'] = re.split(': ', proc_line, maxsplit=1)[1]
                elif re.fullmatch('^"Modelo / Versão: .*"\n$|^"Modelo: .*"\n$', line) is not None: #Modelo / Versão
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Modelo / Versão'] = re.split(': ', proc_line, maxsplit=1)[1]
                elif re.fullmatch('^"Descrição Complementar: .*"\n$|^"Descrição Detalhada do Objeto Ofertado: .*"\n$', line) is not None: #Descrição
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Description_Proposta'] = re.split(': ', proc_line, maxsplit=1)[1]
                elif re.fullmatch('^"Porte da empresa: .*"\n', line) is not None:
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Porte da empresa'] = re.split(': ', proc_line, maxsplit=1)[1]
                elif re.fullmatch('^"Declaração de Origem do Produto: .*"\n', line) is not None:
                    proc_line = line.removeprefix('"')
                    proc_line = proc_line.removesuffix('"\n')
                    self.col_dict2['Declaração de Origem do Produto'] = re.split(': ', proc_line, maxsplit=1)[1]
            elif self.within_sect and not self.within_two and self.within_three:
                three_out_cond = ('^"Não existem lances de desempate ME/EPP para o item"\n$|'
                                  '^"Não existem lances de desempate para o item"\n$|'
                                  '^"Eventos do Item"\n$|'
                                  '^"Desempate de Lances ME/EPP"\n$')
                if re.fullmatch('"Valor do Lance CNPJ/CPF Data/Hora Registro"\n|"Valor do lance R\$ CNPJ/CPF Data"\n', line) is not None:
                    third_temp_param = '1a'
                    continue
                elif re.fullmatch('"Valor do Lance Fator de Equalização Valor do Lance Equalizado CNPJ/CPF Data/Hora Registro"\n', line) is not None:
                    third_temp_param = '7'
                    continue
                elif re.fullmatch('"Desconto Valor com Desconto CNPJ/CPF Data/Hora Registro"\n', line) is not None:
                    third_temp_param = '4b'
                    continue
                elif re.fullmatch(three_out_cond, line) is not None:
                    self.within_three = False
                else:
                    self.col_dict3 = self.third_parser(line, third_temp_param, row_num, cleanse=True)
                    self.df3.append(self.col_dict3)
                    self.col_dict3 = {}


class FourthInfoExtractor:
    '''
    Purpose: Extracts all relevant information for File 4 from ONE txt file

    Initial parameters:
        1. Path to text file
        2. List of dictionaries which will form the final dataframe
    '''

    def __init__(self, path, df):
        self.filepath = path
        self.trunc_filepath = self.filepath.removeprefix('bids/txt/')
        self.df4 = df
        self.col_dict = {}
        self.within_four = False

        with open(path, encoding="ISO-8859-1") as f:
            self.lines = f.readlines()

    def line_parser(self, curr_line, row_num):
        curr_line2 = curr_line.removesuffix('"\n')
        curr_line2 = curr_line2.removeprefix('"')
        tmp_dict = {'FileName': self.trunc_filepath, 'FileRow': row_num}
        tmp_dict['Time'] = datetime.strptime(re.findall('[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]', curr_line2)[0], '%d/%m/%Y %H:%M:%S')
        tmp_dict['Firm ID'] = re.split(' [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] ', curr_line2)[0]
        tmp_dict['Message'] = re.split(' [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] ', curr_line2)[1]
        return tmp_dict

    def extract_and_add(self):
        file_row = 0
        for line in self.lines:
            file_row += 1
            if not self.within_four and re.match('^"Data Mensagem"\n$', line) is not None:
                self.within_four = True
            elif not self.within_four and re.fullmatch(
                    '"Sistema [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*"\n|"Pregoeiro [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*"\n|"[0-9]*.[0-9]*.[0-9]*/[0-9]*-[0-9]* [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*"\n',
                    line) is not None:
                self.within_four = True
                self.col_dict = self.line_parser(line, file_row)
                self.df4.append(self.col_dict)
                self.col_dict = {}
            elif self.within_four and (re.search('Eventos do Pregão', line) is not None or
                                       re.fullmatch(
                                           '".* [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] .*"\n',
                                           line) is None):
                self.within_four = False
            elif self.within_four:
                if re.fullmatch(
                        '".* [0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]"\n|""\n',
                        line) is not None:
                    continue
                else:
                    self.col_dict = self.line_parser(line, file_row)
                    self.df4.append(self.col_dict)
                    self.col_dict = {}

    def update_df(self):
        return self.df4


if __name__ == "__main__":
    print('info_extractors.py NOT MEANT TO BE RUN AS MAIN SCRIPT')