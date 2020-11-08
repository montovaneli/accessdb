# -*- coding: utf-8 -*-
# Copyright 2017 Dhana Babu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import random
import string
import tempfile
import shutil

import pypyodbc as odbc

from .access_api import create


_MS_ACCESS_TYPES = {
    'BIT',
    'BYTE',
    'SHORT',
    'LONG',
    'CURRENCY',
    'SINGLE',
    'DOUBLE',
    'DATETIME',
    'TEXT',
    'MEMO',
    'PRIMARY',  # CUSTOM Type for handling AUTOINCREMENT
}

SCHEMA_FILE = 'schema.ini'

_TEXT_SEPARATORS = {
                   r',': 'CSVDelimited',
                   r'\t': 'TabDelimited'
                   }

_CODE_PAGES_CHARSETS = { 
                        r'IBM037':'037',
                        r'IBM437':'437',
                        r'IBM500':'500',
                        r'ASMO-708':'708',
                        r'ASMO-449':'709',
                        r'TRANSPARENT_ARABIC':'710',
                        r'DOS-720':'720',
                        r'IBM737':'737',
                        r'IBM775':'775',
                        r'IBM850':'850',
                        r'IBM852':'852',
                        r'IBM855':'855',
                        r'IBM857':'857',
                        r'IBM00858':'858',
                        r'IBM860':'860',
                        r'IBM861':'861',
                        r'DOS-862':'862',
                        r'IBM863':'863',
                        r'IBM864':'864',
                        r'IBM865':'865',
                        r'CP866':'866',
                        r'IBM869':'869',
                        r'IBM870':'870',
                        r'WINDOWS-874':'874',
                        r'CP875':'875',
                        r'SHIFT_JIS':'932',
                        r'GB2312':'936',
                        r'KS_C_5601-1987':'949',
                        r'BIG5':'950',
                        r'IBM1026':'1026',
                        r'IBM01047':'1047',
                        r'IBM01140':'1140',
                        r'IBM01141':'1141',
                        r'IBM01142':'1142',
                        r'IBM01143':'1143',
                        r'IBM01144':'1144',
                        r'IBM01145':'1145',
                        r'IBM01146':'1146',
                        r'IBM01147':'1147',
                        r'IBM01148':'1148',
                        r'IBM01149':'1149',
                        r'UTF-16':'1200',
                        r'UNICODEFFFE':'1201',
                        r'WINDOWS-1250':'1250',
                        r'WINDOWS-1251':'1251',
                        r'WINDOWS-1252':'1252',
                        r'WINDOWS-1253':'1253',
                        r'WINDOWS-1254':'1254',
                        r'WINDOWS-1255':'1255',
                        r'WINDOWS-1256':'1256',
                        r'WINDOWS-1257':'1257',
                        r'WINDOWS-1258':'1258',
                        r'JOHAB':'1361',
                        r'MACINTOSH':'10000',
                        r'X-MAC-JAPANESE':'10001',
                        r'X-MAC-CHINESETRAD':'10002',
                        r'X-MAC-KOREAN':'10003',
                        r'X-MAC-ARABIC':'10004',
                        r'X-MAC-HEBREW':'10005',
                        r'X-MAC-GREEK':'10006',
                        r'X-MAC-CYRILLIC':'10007',
                        r'X-MAC-CHINESESIMP':'10008',
                        r'X-MAC-ROMANIAN':'10010',
                        r'X-MAC-UKRAINIAN':'10017',
                        r'X-MAC-THAI':'10021',
                        r'X-MAC-CE':'10029',
                        r'X-MAC-ICELANDIC':'10079',
                        r'X-MAC-TURKISH':'10081',
                        r'X-MAC-CROATIAN':'10082',
                        r'UTF-32':'12000',
                        r'UTF-32BE':'12001',
                        r'X-CHINESE_CNS':'20000',
                        r'X-CP20001':'20001',
                        r'X_CHINESE-ETEN':'20002',
                        r'X-CP20003':'20003',
                        r'X-CP20004':'20004',
                        r'X-CP20005':'20005',
                        r'X-IA5':'20105',
                        r'X-IA5-GERMAN':'20106',
                        r'X-IA5-SWEDISH':'20107',
                        r'X-IA5-NORWEGIAN':'20108',
                        r'US-ASCII':'20127',
                        r'X-CP20261':'20261',
                        r'X-CP20269':'20269',
                        r'IBM273':'20273',
                        r'IBM277':'20277',
                        r'IBM278':'20278',
                        r'IBM280':'20280',
                        r'IBM284':'20284',
                        r'IBM285':'20285',
                        r'IBM290':'20290',
                        r'IBM297':'20297',
                        r'IBM420':'20420',
                        r'IBM423':'20423',
                        r'IBM424':'20424',
                        r'X-EBCDIC-KOREANEXTENDED':'20833',
                        r'IBM-THAI':'20838',
                        r'KOI8-R':'20866',
                        r'IBM871':'20871',
                        r'IBM880':'20880',
                        r'IBM905':'20905',
                        r'IBM00924':'20924',
                        r'EUC-JP':'20932',
                        r'X-CP20936':'20936',
                        r'X-CP20949':'20949',
                        r'CP1025':'21025',
                        r'KOI8-U':'21866',
                        r'ISO-8859-1':'28591',
                        r'ISO-8859-2':'28592',
                        r'ISO-8859-3':'28593',
                        r'ISO-8859-4':'28594',
                        r'ISO-8859-5':'28595',
                        r'ISO-8859-6':'28596',
                        r'ISO-8859-7':'28597',
                        r'ISO-8859-8':'28598',
                        r'ISO-8859-9':'28599',
                        r'ISO-8859-13':'28603',
                        r'ISO-8859-15':'28605',
                        r'X-EUROPA':'29001',
                        r'ISO-8859-8-I':'38598',
                        r'ISO-2022-JP':'50220',
                        r'CSISO2022JP':'50221',
                        r'ISO-2022-JP':'50222',
                        r'ISO-2022-KR':'50225',
                        r'X-CP50227':'50227',
                        r'EUC-JP':'51932',
                        r'EUC-CN':'51936',
                        r'EUC-KR':'51949',
                        r'HZ-GB-2312':'52936',
                        r'GB18030':'54936',
                        r'X-ISCII-DE':'57002',
                        r'X-ISCII-BE':'57003',
                        r'X-ISCII-TA':'57004',
                        r'X-ISCII-TE':'57005',
                        r'X-ISCII-AS':'57006',
                        r'X-ISCII-OR':'57007',
                        r'X-ISCII-KA':'57008',
                        r'X-ISCII-MA':'57009',
                        r'X-ISCII-GU':'57010',
                        r'X-ISCII-PA':'57011',
                        r'UTF-7':'65000',
                        r'UTF-8':'65001'
                        }


def _text_formater(sep):
    separator = _TEXT_SEPARATORS.get(sep, 'Delimited({})')
    return separator.format(sep)

def _define_charset(code):
    charset = _CODE_PAGES_CHARSETS.get(code, '65001')
    return f'CharacterSet={charset}'

def _stringify_path(db_path):
    dtr, path = os.path.split(db_path)
    if dtr == '':
        db_path = os.path.join('.', path)
    return db_path


def _push_access_db(temp_dir, text_file, data_columns,
                    header_columns, dtype, path, table_name, sep,
                    append, overwrite, charset, delete='file'):
    table = Table(temp_dir, text_file,
                  table_name,
                  data_columns,
                  header_columns,
                  dtype, sep, append, charset)
    schema_file = os.path.join(temp_dir, SCHEMA_FILE)
    try:
        with SchemaWriter(temp_dir, text_file, data_columns,
                          header_columns, dtype, sep, schema_file, charset) as schema:
            schema.write()
        with AccessDBConnection(path, overwrite) as con:
            cursor = con.cursor()
            if not append:
                cursor.execute(table.create_query())
            cursor.execute(table.insert_query())
            con.commit()
    finally:
        if delete == 'folder':
            shutil.rmtree(temp_dir)
        else:
            os.unlink(schema_file)


def _get_random_file():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(10))


class DataTypeNotFound(Exception):
    pass


class SchemaWriter(object):
    def __init__(self, temp_dir, text_file, df_columns,
                 columns, dtype, sep, schema_file, charset):
        self.temp_dir = temp_dir
        self.text_file = text_file
        self.df_columns = df_columns
        self.columns = columns
        self.dtype = dtype
        self.sep = sep
        self.path = schema_file
        self.charset = charset

    def __enter__(self):
        self.fp = open(self.path, 'w')
        return self

    def __exit__(self, *args):
        self.fp.close()

    def formater(self):
        yield '[%s]' % self.text_file
        yield 'ColNameHeader=True'
        yield _define_charset(self.charset)
        yield 'Format=%s' % _text_formater(self.sep)
        self.dcols = {col: ('Col%s' % (i + 1))
                      for i, col in enumerate(self.df_columns)}
        if not isinstance(self.dtype, dict):
            self.dtype = {}
        for col in self.df_columns:
            ctype = self.dtype.get(col, 'text').upper()
            if ctype not in _MS_ACCESS_TYPES:
                raise DataTypeNotFound(
                    'Provided Data Type Not Found %s' % ctype)
            if ctype == 'PRIMARY':
                ctype = 'TEXT'
            yield '{c_col}="{d_col}" {c_type}'.format(
                                                c_col=self.dcols[col],
                                                d_col=col,
                                                c_type=ctype.capitalize())

    def write(self):
        for line in self.formater():
            self.fp.write(line)
            self.fp.write('\n')


class Table(object):
    def __init__(self, temp_dir, text_file,
                 table_name, df_columns, columns,
                 dtype, sep, append):
        self.temp_dir = temp_dir
        self.text_file = text_file
        self.df_columns = df_columns
        self.table_name = table_name
        self.df_columns = df_columns
        self.columns = columns
        self.dtype = dtype
        self.sep = sep
        self.append = append
        if not isinstance(self.dtype, dict):
            self.dtype = {}

    def _get_colunm_type(self, col):
        ctype = self.dtype.get(col, 'TEXT').upper()
        if ctype not in _MS_ACCESS_TYPES:
            raise Exception
        return ctype

    def formater(self):
        for col in self.df_columns:
            c_type = self._get_colunm_type(col)
            if c_type == 'PRIMARY':
                c_type = 'AUTOINCREMENT PRIMARY KEY'
            if self.columns:
                if col not in self.columns:
                    continue
                col = self.columns[col]
            yield '`{c_col}`  {c_type}'.format(c_col=col,
                                               c_type=c_type)

    def insert_formater(self):
        for col in self.df_columns:
            if self._get_colunm_type(col) == 'PRIMARY':
                continue
            if not self.columns:
                self.columns = dict(zip(self.df_columns, self.df_columns))
            if self.columns:
                if col not in self.columns:
                    continue
                cus_col = self.columns[col]
            yield col, cus_col

    def built_columns(self):
        return '(%s)' % ','.join(self.formater())

    def create_query(self):
        return "CREATE TABLE `{table_name}`{columns}".format(
                                            table_name=self.table_name,
                                            columns=self.built_columns())

    @staticmethod
    def required_columns(cols):
        return ','.join('`%s`' % c for c in cols)

    def insert_query(self):
        custom_columns = []
        columns = []
        for col1, col2 in self.insert_formater():
            columns.append(col1)
            custom_columns.append(col2)
        return """
                INSERT INTO `{table_name}`({columns})
                    SELECT {required_cols}  FROM [TEXT;HDR=YES;FMT={separator};
                                    Database={temp_dir}].{text_file}
            """.format(temp_dir=self.temp_dir,
                       text_file=self.text_file,
                       columns=self.required_columns(custom_columns),
                       required_cols=self.required_columns(columns),
                       table_name=self.table_name,
                       separator=_text_formater(self.sep))


class AccessDBConnection(object):
    def __init__(self, db_path, overwrite):
        self.overwrite = overwrite
        self.db_path = _stringify_path(db_path)

    def __enter__(self):
        if not os.path.isfile(self.db_path) or self.overwrite:
            create(self.db_path)
        odbc_conn_str = '''DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};
                           DBQ=%s''' % (self.db_path)
        self.con = odbc.connect(odbc_conn_str)
        return self.con

    def __exit__(self, *args):
        self.con.close()


def to_accessdb(self, path, table_name,
                header_columns=None, dtype='str', engine='text',
                sep=',', append=False, overwrite=False, charset='UTF-8'):
    if self.empty:
        return
    temp_dir = tempfile.mkdtemp()
    text_file = '%s.txt' % _get_random_file()
    text_path = os.path.join(temp_dir, text_file)
    self.to_csv(text_path, index=False)
    _push_access_db(temp_dir, text_file,
                    self.columns.tolist(),
                    header_columns, dtype, path, table_name,
                    sep, append, overwrite, charset, 'folder')


def create_accessdb(path, text_path, table_name,
                    header_columns=None, dtype='str',
                    engine='text', sep=',', append=False, overwrite=False, charset='UTF-8'):
    temp_dir, text_file = os.path.split(os.path.abspath(text_path))
    with open(text_path) as fp:
        file_columns = fp.readline().strip('\n').split(sep)
    _push_access_db(temp_dir, text_file,
                    file_columns,
                    header_columns, dtype, path, table_name,
                    sep, append, overwrite, charset)
