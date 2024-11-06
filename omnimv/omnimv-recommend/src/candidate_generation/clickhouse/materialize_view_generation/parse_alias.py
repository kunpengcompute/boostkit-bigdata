#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

class AliasParser(object):
    def __init__(self, _item_exp):
        self.item_exp = _item_exp

    def get_name_alias(self):
        if ' as ' in self.item_exp and (
                ')' not in self.item_exp or
                self.item_exp.rindex(' as ') > self.item_exp.rindex(')')):
            old_alias = self.item_exp.split(' as ')[-1].strip()
            item_name = self.item_exp.replace(' as ' + old_alias, '').strip()
        elif ' AS ' in self.item_exp and (
                ')' not in self.item_exp or
                self.item_exp.rindex(' AS ') > self.item_exp.rindex(')')):
            old_alias = self.item_exp.split(' AS ')[-1].strip()
            item_name = self.item_exp.replace(' AS ' + old_alias, '').strip()
        elif self.__contains_whitespace_alia():
            item_name, old_alias = self.__parse_whitespace_alia()
        else:
            item_name = self.item_exp
            old_alias = ''
        return item_name.strip(), old_alias.strip()

    def __contains_whitespace_alia(self):
        item = self.item_exp.strip()
        if ' ' in item:
            if ')' in item and item.rindex(' ') < item.rindex(')'):
                return False  # 最后一个空格在括号里
            words = item.split(' ')
            true_words = []  # 非空格非空token
            for word in words:
                if word != '':
                    true_words.append(word)
            if len(true_words) < 2:
                return False
            alia = true_words[-1]
            if alia.lower().strip() == 'end':
                return False  # case when then else end 不一定有别名
            the_front_word = true_words[-2]
            tail = the_front_word[-1]
            if tail in [')', '_', '\''] or tail.isalpha() or tail.isdigit():
                return True
        return False

    def __parse_whitespace_alia(self):
        item = ' ' + self.item_exp + ' '
        words = item.split(' ')
        true_words = []
        for word in words:
            if word != '':
                true_words.append(word)
        alias = true_words[-1]

        alia_index = item.rindex(alias)
        name = item[0:alia_index]
        return name, alias