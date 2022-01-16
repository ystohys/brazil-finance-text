import re
from copy import deepcopy


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
        for line in self.lines:
            if line.count(': ') > 1:
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
            elif line.count(': ') == 1:
                if re.fullmatch('^"Item: [0-9][0-9]*"\n$', line) is not None and self.within_row == False:
                    self.within_row = True
                    self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                elif (self.within_row or self.stop_count >= 0) and (
                        re.fullmatch('^"Aceito para: .* \."\n$', line) is not None or
                        re.fullmatch('^"Adjudicado para: .*"\n$', line) is not None):
                    self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                    self.within_row = False
                    self.df1.append(self.col_dict)
                    self.col_dict = {'FileName': self.trunc_filepath}
                elif (self.within_row or self.stop_count >= 0):
                    if re.fullmatch('""\n|^"Descrição Complementar:"\n$', line) is not None:
                        continue
                    else:
                        self.col_dict = self.line_split_to_dict(line, self.col_dict, cleanse=True)
                        if self.stop_count == 0:
                            self.df1.append(self.col_dict)
                            self.col_dict = {'FileName': self.trunc_filepath}
            self.stop_count -= 1

