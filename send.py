#! -*- coding=utf-8 -*-
import os
import shutil
from lxml import etree
from datetime import datetime
import uuid
from string import Template
import re
import pytz
from dateutil import parser as dateutil_parser
import time
import json
import pickle
import sqlite3
import smtplib
import ConfigParser
from email.mime.text import MIMEText

def str2int(s, default=0):
    u"""
    Переводит строку s в целое число. Если это не удается (s не содержит
    числа), возвращает значение default.

    Параметры:
    * s - строка, из которой нужно получить число;
    * default - результат по-умолчанию, целое число.

    Примеры использования:
    >>> str2int("45")
    45
    >>> str2int("45см")
    45
    >>> str2int("Рост: 45 см")
    45
    >>> str2int("Рост: -", 50)
    50
    """
    return int((re.findall(r'\d+', s) or [default])[0])

def guid_separate(a, default=0):
    return a[0:8] + "-" + a[8:12] + "-" + a[12:16] + "-" + a[16:20] + "-" + a[20:]

class MedoMesage(object):
    def __init__(self, message_uid, message_broker, current_process_dir):
        self._message_uid = message_uid
        self._message_broker = message_broker
        self._DIRS = message_broker._DIRS
        self._is_failure = False
        self._logger = Logger()
        self._current_process_dir = current_process_dir
        # Проверим с чем имеем дело - сообщение/квитанция
        if os.path.exists(os.path.join(self._current_process_dir, message_uid, 'DocInfo.xml')):
            # Сообщение
            self._is_document = True
            self._xml_filename = os.path.join(self._current_process_dir, message_uid, 'DocInfo.xml')
            self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'delo_docinfo_to_medo_document.xslt')
            self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'IEDMS.xsd')

        elif os.path.exists(os.path.join(self._current_process_dir, message_uid, 'Report.xml')):
            # Квитанция
            self._is_document = False
            self._xml_filename = os.path.join(self._current_process_dir, message_uid, 'Report.xml')
            # Загрузим файл Документа/Квитанции в формате ДЕЛО
            tree = etree.parse(self._xml_filename)
            
            # Определяем точный тип квитанции и шаблоны ее обработки
            namespaces = {'sev' : 'http://www.eos.ru/2010/sev'} 
            reception = tree.findall("*/sev:Reception", namespaces)
            document_accepted = tree.findall("*/sev:Registration", namespaces)
            document_refused = tree.findall("*/sev:Failure", namespaces)

            if reception:
                self._is_acknowledgment = True
                self._is_notification = False
                self._is_notification_confirm = False
                self._is_notification_refuse = False
                self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'delo_report_to_medo_acknowledgment.xslt')
            else:
                self._is_notification = True
                self._is_acknowledgment = False
                if document_accepted:
                    self._is_notification_confirm = True
                    self._is_notification_refuse = False
                    self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'delo_report_to_medo_notification_confirm.xslt')
                if document_refused:                
                    self._is_notification_refuse = True
                    self._is_notification_confirm = False
                    self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'delo_report_to_medo_notification_refuse.xslt')

            self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'IEDMS.xsd')
        else:
            raise Exception('Unknown type of DELO message %s' % message_uid)

    def _move_message_to_archive(self):
        "Перемещение ДЕЛО сообщения в архивную папку"
        try:
            if self._message_broker._is_subdir_date:
                datestamp = datetime.now().strftime('%Y_%m_%d')
                if not os.path.exists(os.path.join(self._DIRS['ARCHIVE_DIR'], datestamp)):
                    os.makedirs(os.path.join(self._DIRS['ARCHIVE_DIR'], datestamp))
                # Переместим файл .env
                shutil.move(os.path.join(self._current_process_dir, '%s.env' % self._message_uid), os.path.join(self._DIRS['ARCHIVE_DIR'],datestamp))
                # Переместим каталог с сообщением
                shutil.move(os.path.join(self._current_process_dir, self._message_uid), os.path.join(self._DIRS['ARCHIVE_DIR'],datestamp))
            else:
                # Переместим файл .env
                shutil.move(os.path.join(self._current_process_dir, '%s.env' % self._message_uid), self._DIRS['ARCHIVE_DIR'])
                # Переместим каталог с сообщением
                shutil.move(os.path.join(self._current_process_dir, self._message_uid), self._DIRS['ARCHIVE_DIR'])        
        except Exception, e:
            self._logger._log('Error: Directory ' + os.path.join(self._DIRS['RECEIVE_DIR'], self._message_uid) + 'already exists. ' + e.message)

        # Если в директориях типа "2014.12.32 08:12:32" все файлы обработаны, то перемещаем сами эти директории
        if not os.listdir(self._current_process_dir):
            if self._message_broker._is_subdir_date:
                shutil.move(self._current_process_dir, os.path.join(self._DIRS['ARCHIVE_DIR'], datestamp))
            else:
                shutil.move(self._current_process_dir, self._DIRS['ARCHIVE_DIR'])

    def _create_envelope_ini(self):
        "Создание файла envelope.ini"
        if self._is_document:
            # Получим тему сообщения из МЭДО сообщения
            namespaces = {'xdms' : 'http://www.infpres.com/IEDMS'} 
            annotation = self._medo_message_tree.findall("*/xdms:annotation", namespaces)[0].text

            # Получим список приложенных файлов из МЭДО сообщения
            applied_files = self._medo_message_tree.findall("*/xdms:file", namespaces)
            applied_files_list = []
            for applied_file in applied_files:
                # Получим номер файла для записи в envelope.ini В МЭДО сообщении нумерация с 0
                # В envelope.ini нулевой файл - document.xml, все приложения нумеруются с 1
                # поэтому увеличиваем номер файла из МЭДО сообщения на 1
                file_id = int(applied_file.attrib['{%s}localId' % namespaces['xdms']]) + 1
                # Получим имя приложенного файла
                filename = applied_file.attrib['{%s}localName' % namespaces['xdms']]
                # Получим расширение приложенного файла
                filetype = applied_file.attrib['{%s}type' % namespaces['xdms']]
                # Добавим номер файла и имя файла в список
                applied_files_list.append([file_id, filename])
            
            # Отсортируем список приложенных файлов по их номерам
            applied_files_list.sort()
            # Соберем строку с перечислением приложенных файлов
            files_str = u"0=document.xml\r\n"
            for file_id, filename in applied_files_list:
                files_str += '%s=%s\r\n' % (file_id, filename)

        else:
            # Квитанция
            if self._is_acknowledgment:
                annotation = u'Квитанция'
                files_str = u'0=acknowledgment.xml'
            # Уведомление
            if self._is_notification:
                annotation = u'Квитанция'
                files_str = u'0=notification.xml'

        # Подготовим атрибуты для подстановки
        attrs = {'TITLE' : annotation.encode('cp1251'),
                 'DATETIME': datetime.now().strftime('%d.%m.%Y %H:%M:%S').encode('cp1251'),
                 'FILES' : files_str.encode('cp1251')}

        # Сформируем содержимое файла envelope.ini на основании шаблона
        with open(os.path.join(self._DIRS['TEMPLATES_DIR'], 'envelope.ini'), 'rb') as f:
            envelope_content = Template(f.read()).substitute(attrs)

        # Запишем файл envelope.ini
        with open(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid, 'envelope.ini'), 'wb') as f:
            f.write(envelope_content)

    def _copy_applied_files(self):
        "Копирование приложенных файлов ДЕЛО сообщения в директорию МЭДО сообщения"
        for filename in os.listdir(os.path.join(self._current_process_dir, self._message_uid)):
            # Не нужно копировать файл DocInfo.xml Report.xml и файлы электронных подписей .sig
            if filename != 'DocInfo.xml' and filename != 'Report.xml' and not filename.endswith('.sig'):
                shutil.copy(os.path.join(self._current_process_dir, 
                                         self._message_uid, 
                                         filename), 
                            os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid, filename))

    def _create_remote_folders(self):
        # Создадим директорию для МЭДО сообщения
        if not os.path.exists(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid)):
            os.makedirs(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid))

    def _save_medo_message(self):
        "Запись МЭДО сообщения в файл в директории отправки МЭДО"
        if self._is_document:
            # Документ
            full_xml_filename = os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid, 'document.xml')
        else:
            # Квитанция
            if self._is_acknowledgment:
                full_xml_filename = os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid, 'acknowledgment.xml')
            if self._is_notification:
                full_xml_filename = os.path.join(self._DIRS['MEDO_SEND_DIR'], self._transport_guid, 'notification.xml')

        # Запишем МЭДО сообщение в файл
        with open(full_xml_filename, 'wb') as f:
            self._medo_message_tree.write(f, pretty_print = True, encoding='windows-1251')

    def _create_message(self):
        "Создание МЭДО сообщения из ДЕЛО сообщения/квитанции"
        # Загрузим файл Документа/Квитанции в формате ДЕЛО
        tree = etree.parse(self._xml_filename)
        # Загрузим шаблон преобразования Документа/Квитанции из дормата ДЕЛО в формат МЭДО
        xslt = etree.parse(self._xslt_template_filename)
        # Создадим функцию преобразования
        transform = etree.XSLT(xslt)
        # Получим преобразованный Документ/Квитанцию в формате МЭДО
        result_tree = transform(tree)

        if not self._is_document:
            # Добавим недостающую дополнительную информацию
            transport_guid = tree.xpath('/sev:Report/sev:Header/@ReturnID', namespaces={'sev':"http://www.eos.ru/2010/sev"})[0]
            department_uid = tree.xpath('..//sev:Report/sev:Header/sev:Sender/sev:Contact/sev:Organization/@UID', namespaces={'sev':"http://www.eos.ru/2010/sev"})[0]
            message_info = self._message_broker.get_message_info(transport_guid);
            if message_info:
                document_uid = message_info[2].encode("utf-8");
                # Уведомление 
                if self._is_acknowledgment:
                    date_time = result_tree.xpath('.//xdms:acknowledgment/xdms:time', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0]
                    date_time.text = date_time.text[:19]
                if self._is_notification_refuse:
                    date_time = result_tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:time', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0]
                    date_time.text = date_time.text[:19]
                if self._is_notification_confirm:
                    date_time = result_tree.xpath('.//xdms:notification/xdms:documentAccepted/xdms:time', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0]
                    date_time.text = date_time.text[:19]

                # Определим значение дополнительных атрибутов, которые будут подставлены в МЭДО сообщение
                if self._is_acknowledgment:
                    attrs = {   'MESSAGE_UID' : str(uuid.uuid4()).upper(), # Транспортный GUID, он в примере UPPER CASE
                                'DOCUMENT_UID' : document_uid, # GUID документа
                                'MSG_TYPE': u'Квитанция',
                                'AGV_GUID' : department_uid # GUID Администрации города Вологды. Постоянный.
                                }
                if self._is_notification:
                    attrs = {   'MESSAGE_UID' : str(uuid.uuid4()).upper(), # Транспортный GUID, он в примере UPPER CASE
                                'DOCUMENT_UID' : document_uid, # GUID документа
                                'AGV_GUID' : department_uid # GUID Администрации города Вологды. Постоянный.
                                }
            else:
                self._is_failure = True
                self._logger._log("The Acknowledgment with GUID: " + transport_guid + " has no initial document in delo database. Document will not process.")
        else: 
            transport_guid = result_tree.xpath('.//xdms:header/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
            sign_date = result_tree.xpath('.//xdms:signatories/xdms:signatory/signdate', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
            government_guid = result_tree.xpath('.//xdms:header/xdms:source/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
            message_guid = tree.xpath('.//sev:Document/@UID', namespaces={'sev':"http://www.eos.ru/2010/sev"})[0]
            department_uid = tree.xpath('..//sev:DocInfo/sev:Header/sev:Sender/sev:Contact/sev:Organization/@UID', namespaces={'sev':"http://www.eos.ru/2010/sev"})[0]
            # Документ
            # Определим значение дополнительных атрибутов, которые будут подставлены в МЭДО сообщение
            attrs = {   'ISO_DATETIME' : datetime.now().replace(microsecond=0).isoformat(), # Время формирования сообщения
                        'TRANSPORT_GUID' : str(uuid.uuid4()).upper(), # Транспортный GUID, он в примере UPPER CASE
                        'MESSAGE_GUID' : guid_separate(message_guid), #str(uuid.uuid4()), # GUID документа
                        'AGV_GUID' : department_uid # GUID Администрации города Вологды. Постоянный.
                        }
        if not self._is_failure:
            # Получим МЭДО сообщение в виде строки
            result_string = etree.tostring(result_tree, encoding='utf-8')
            # Подставим в МЭДО сообщение значения дополнительных атрибутов и сформируем итоговый XML
            # Формировать итоговый XML нужно для того, чтобы атрибут "ЧИСЛО СТРАНИЦ" был integer (в ДЕЛО сообщении может быть строка),
            # а также проверить МЭДО сообщение по XSD схеме МЭДО сообщения 
            result_tree = etree.fromstring(Template(result_string).substitute(attrs)).getroottree()

        if self._is_document:
            # Найдем атрибут "ЧИСЛО_СТРАНИЦ"
            namespaces = {'xdms' : 'http://www.infpres.com/IEDMS'} 
            pages = result_tree.findall("*/xdms:pages", namespaces)[0]

            # Запишем в атрибут "ЧИСЛО_СТРАНИЦ" значение типа integer
            try:
                pages.text = unicode(str2int(pages.text))
            except Exception, e:
                self._logger._log('Exception: ' + e.message)
                self._logger._log('Error Comment: It\'s possible, that the author of the message didn\'t set number of pages.')

        if not self._is_failure:
            # Загрузим XSD схему для проверки МЭДО сообщения
            xmlschema_doc = etree.parse(self._xsd_filename)
            xmlschema = etree.XMLSchema(xmlschema_doc)

            # Проверим МЭДО сообщение по XSD схеме
            if xmlschema.validate(result_tree):
                # Проверка прошла успешно, можно сохранять сообщение
                if self._is_document:
                    self._transport_guid = attrs['TRANSPORT_GUID']
                else:
                    self._transport_guid = attrs['MESSAGE_UID']
                self._medo_message_tree = result_tree

                # Сохраним информацию об исходном письме для последующего формирования квитанции для ДЕЛО
                if self._is_document:
                    return_id = tree.xpath('.//sev:Header/@ReturnID', 
                                            namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0]            

                    document_uid = result_tree.xpath('.//xdms:document/@xdms:uid', 
                                            namespaces = {'xdms':"http://www.infpres.com/IEDMS"})[0]

                    document_group = tree.xpath('.//sev:DocumentList/sev:Document/sev:Group', 
                                            namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0].text

                    registration_number = tree.xpath('.//sev:DocumentList/sev:Document/sev:RegistrationInfo/sev:Number', 
                                            namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0].text

                    registration_date = tree.xpath('.//sev:DocumentList/sev:Document/sev:RegistrationInfo/sev:Date', 
                                            namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0].text

                    sender_contact = tree.xpath('*/sev:Sender/sev:Contact', namespaces = {'sev': "http://www.eos.ru/2010/sev"})
                    recipient_contact = tree.xpath('*/sev:Recipient/sev:Contact', namespaces = {'sev': "http://www.eos.ru/2010/sev"})
                    author = tree.xpath('*/sev:Document/sev:Author', namespaces = {'sev': "http://www.eos.ru/2010/sev"})


                    sender_contact_pickle = pickle.dumps(map(lambda x: etree.tostring(x, encoding='utf-8'), sender_contact))
                    recipient_contact_pickle = pickle.dumps(map(lambda x: etree.tostring(x, encoding='utf-8'), recipient_contact))
                    author_pickle = pickle.dumps(map(lambda x: etree.tostring(x, encoding='utf-8'), author))

                    kwargs = {'return_id' : return_id,
                              'document_uid' : document_uid,
                              'sender_contact_pickle' : sender_contact_pickle,
                              'recipient_contact_pickle' : recipient_contact_pickle,
                              'author_pickle' : author_pickle,
                              'registration_number' : registration_number,
                              'registration_date': registration_date,
                              'document_group': document_group}

                    self._message_broker.set_message_info(self._transport_guid, "delo", **kwargs)
                return u'Success'
            else:
                # Проверка не прошла, выведем причину непрохождения
                try:
                    xmlschema.assertValid(result_tree)
                except Exception, e:
                    self._logger._log('XSD Error: ' + e.message)

                return None
        else:
            return None

    def send(self):
        result = self._create_message()

        if result:
            # Создадим директории для передачи сообщения
            self._create_remote_folders()
            # Скопируем в директорию МЭДО сообщения приложенные файлы ДЕЛО сообщения
            self._copy_applied_files()
            # Сохраним сообщение в директорию отправки МЭДО
            self._save_medo_message()
            # Создадим в директории МЭДО сообщения файл envelope.ini
            self._create_envelope_ini()
            # Переместим сообщение ДЕЛО в архивнут папку, чтобы не отправлять его еще раз
            self._move_message_to_archive()

class DELOMessage(object):
    def __init__(self, message_uid, message_broker):
        self._message_uid = message_uid
        self._message_broker = message_broker
        self._DIRS = message_broker._DIRS
        self._is_failure = False
        self._logger = Logger()
        # Если активирована опция для формирования уведомлений со стороны демона, то установим соответствующий шаблон
        if self._message_broker._is_custom_medo_acknowledgment:
            self._xslt_template_filename_for_custom_acknowledgment = os.path.join(self._DIRS['TEMPLATES_DIR'], 'medo_document_to_medo_acknowledgment.xslt')
            self._xsd_acknowledgment_filename = os.path.join(self._DIRS['TEMPLATES_DIR'],'IEDMS.xsd')

        # Проверим с чем имеем дело - сообщение/квитанция
        if os.path.exists(os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'document.xml')):
            # Сообщение
            self._is_document = True
            self._xml_filename = os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'document.xml')
            self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'medo_document_to_delo_docinfo.xslt')
            self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'DocumentInfo.xsd')

        elif os.path.exists(os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'acknowledgment.xml')):
            # Квитанция о получении
            self._is_document = False
            self._is_notification = False
            self._is_acknowledgment = True
            self._xml_filename = os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'acknowledgment.xml')
            self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'medo_acknowledgment_to_delo_report.xslt')
            self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'ReportInfo.xsd')

        elif os.path.exists(os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'notification.xml')):
            # Уведомление о регистрации или отказе от регистрации
            self._is_document = False
            self._is_notification = True
            self._is_acknowledgment = False
            self._is_document = False
            self._xml_filename = os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'notification.xml')
            # Загрузим файл Документа/Уведомления в формате МЭДО
            tree = etree.parse(self._xml_filename)

            namespaces = {'xdms' : "http://www.infpres.com/IEDMS"} 
            document_accepted = tree.findall("*/xdms:documentAccepted", namespaces)
            document_refused = tree.findall("*/xdms:documentRefused", namespaces)
            
            # Если это уведомление об успешной регистрации, выберем шаблоны
            if document_accepted:
                self._is_notification_confirm = True
                self._is_notification_refuse = False
                self._xml_filename = os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'notification.xml')
                self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'medo_notification_to_delo_report_confirm.xslt')
                self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'ReportInfo.xsd')
            # Если это уведомление об отказе в регистрации, выберем шаблоны
            if document_refused:
                self._is_notification_confirm = False
                self._is_notification_refuse = True
                self._xml_filename = os.path.join(self._DIRS['RECEIVE_DIR'], message_uid, 'notification.xml')
                self._xslt_template_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'medo_notification_to_delo_report_refuse.xslt')
                self._xsd_filename = os.path.join(self._DIRS['TEMPLATES_DIR'], 'ReportInfo.xsd')

        else:
            raise Exception('Unknown type of MEDO message %s' % message_uid)

    def _move_message_to_archive(self):
        "Перемещение МЭДО сообщения в архивную папку"
        # Переместим каталог с сообщением
        try:
            if self._message_broker._is_subdir_date:
                datestamp = datetime.now().strftime('%Y_%m_%d')
                if not os.path.exists(os.path.join(self._DIRS['ARCHIVE_MEDO_DIR'], datestamp)):
                    os.makedirs(os.path.join(self._DIRS['ARCHIVE_MEDO_DIR'],datestamp))
                shutil.move(os.path.join(self._DIRS['RECEIVE_DIR'], self._message_uid), os.path.join(self._DIRS['ARCHIVE_MEDO_DIR'],datestamp))    
            else:
                shutil.move(os.path.join(self._DIRS['RECEIVE_DIR'], self._message_uid), self._DIRS['ARCHIVE_MEDO_DIR'])
        except Exception, e:
            self._logger._log('Error: Directory ' + os.path.join(self._DIRS['RECEIVE_DIR'], self._message_uid) + 'already exists. ' + e.message)

    def _create_env(self):
        "Создание файла .env"
        if self._is_document:
            # Получим тему сообщения из ДЕЛО сообщения
            namespaces = {'sev' : 'http://www.eos.ru/2010/sev'} 
            attrs = {'SUBJECT':  self._delo_message_tree.findall("*/sev:Document/sev:RegistrationInfo/sev:Number", namespaces)[0].text,
                     'TRANSPORT_GUID' : self._transport_guid,
                     'DATETIME': datetime.now(pytz.timezone('Etc/GMT-3')).isoformat()}
        else:
            # Квитанция
            attrs = {'TRANSPORT_GUID' : self._transport_guid,
                     'DATETIME': datetime.now(pytz.timezone('Etc/GMT-3')).isoformat()
                     }
            # annotation = u'Квитанция'

        # Подготовим атрибуты для подстановки

        # Сформируем содержимое файла envelope.ini на основании шаблона
        with open(os.path.join(self._DIRS['TEMPLATES_DIR'], 'delo.env'), 'rb') as f:
            envelope_content = Template(f.read()).substitute(attrs)

        # Запишем файл envelope.ini
        if self._is_document:
            with open(os.path.join(self._DIRS['DELO_RECEIVE_DIR'], '%s.env' % self._transport_guid), 'wb') as f:
                f.write(envelope_content)
        else:
            with open(os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], '%s.env' % self._transport_guid), 'wb') as f:
                f.write(envelope_content)

    def _copy_applied_files(self):
        "Копирование приложенных файлов МЭДО сообщения в директорию ДЕЛО сообщения"
        for filename in os.listdir(os.path.join(self._DIRS['RECEIVE_DIR'], self._message_uid)):
            # Не нужно копировать файл document.xml acknowledgment.xml envelope.ini
            if filename != 'document.xml' and filename != 'acknowledgment.xml' and filename != 'notification.xml' and filename != 'envelope.ini':
                shutil.copy(os.path.join(self._DIRS['RECEIVE_DIR'], 
                                         self._message_uid, 
                                         filename), 
                            os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid, filename))

    def _create_delo_folders(self):
        if self._is_document:
            # Документ
            if not os.path.exists(os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid)):
                os.makedirs(os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid))
        else:
            # Квитанция/Уведомление
            if not os.path.exists(os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], self._transport_guid)):
                os.makedirs(os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], self._transport_guid))

    def _save_delo_message(self):
        "Запись ДЕЛО сообщения в файл в директории приема ДЕЛО"
        if self._is_document:
            # Создадим директорию для МЭДО сообщения
            # Документ
            if not os.path.exists(os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid)):
                os.makedirs(os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid))

            full_xml_filename = os.path.join(self._DIRS['DELO_RECEIVE_DIR'], self._transport_guid, 'DocInfo.xml')
        else:
            # Квитанция/Уведомление
            if not os.path.exists(os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], self._transport_guid)):
                os.makedirs(os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], self._transport_guid))

            full_xml_filename = os.path.join(self._DIRS['DELO_RECEIVE_DIR_REPORT'], self._transport_guid, 'Report.xml')

        # Запишем МЭДО сообщение в файл
        with open(full_xml_filename, 'wb') as f:
            self._delo_message_tree.write(f, pretty_print = True, encoding='utf-8')

    def _save_custom_medo_acknowledgment(self):
        if self._custom_ack_result_tree:
            if not os.path.exists(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._custom_ack_transport_guid)):
                os.makedirs(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._custom_ack_transport_guid))
            
            full_xml_filename = os.path.join(self._DIRS['MEDO_SEND_DIR'], self._custom_ack_transport_guid, 'acknowledgment.xml')
            with open(full_xml_filename, 'wb') as f:
                self._custom_ack_result_tree.write(f, pretty_print = True, encoding='utf-8')
            return True
        else:
            return False

    def _save_custom_medo_envelope(self):
        if self._custom_ack_result_tree:
            annotation = u'Квитанция'
            files_str = u'0=acknowledgment.xml'
        # Подготовим атрибуты для подстановки
        attrs = {'TITLE' : annotation.encode('cp1251'),
                 'DATETIME': datetime.now().strftime('%d.%m.%Y %H:%M:%S').encode('cp1251'),
                 'FILES' : files_str.encode('cp1251')}
        # Сформируем содержимое файла envelope.ini на основании шаблона
        with open(os.path.join(self._DIRS['TEMPLATES_DIR'], 'envelope.ini'), 'rb') as f:
            envelope_content = Template(f.read()).substitute(attrs)
        # Запишем файл envelope.ini
        with open(os.path.join(self._DIRS['MEDO_SEND_DIR'], self._custom_ack_transport_guid, 'envelope.ini'), 'wb') as f:
            f.write(envelope_content)

    def _create_message(self):
        "Создание ДЕЛО сообщения из МЭДО Документа/Квитанции"
        # Загрузим файл Документа/Квитанции в формате МЭДО
        tree = etree.parse(self._xml_filename)
        # Загрузим шаблон преобразования Документа/Квитанции из дормата ДЕЛО в формат МЭДО
        xslt = etree.parse(self._xslt_template_filename)
        # Создадим функцию преобразования
        transform = etree.XSLT(xslt)
        # Получим преобразованный Документ/Квитанцию в формате ДЕЛО
        result_tree = transform(tree)

        if not self._is_document:
            # Добавим недостающую дополнительную информацию
            if self._is_acknowledgment:
                transport_guid = tree.xpath('.//xdms:acknowledgment/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
                message_info = self._message_broker.get_message_info(transport_guid[0])
            if self._is_notification:
                transport_guid = tree.xpath('.//xdms:notification/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
                message_info = self._message_broker.get_message_info_by_document_uid(transport_guid[0])#.replace("-",""))

                
            if message_info:
                # Получим сохраненные значения из message_info
                return_id = message_info[2].encode('utf-8')
                document_uid = message_info[5].encode('utf-8')
                document_uid = document_uid.replace("-","")
                document_reg_number = message_info[3].encode('utf-8')
                document_reg_date = message_info[4].encode('utf-8')
                document_group = message_info[9].encode('utf-8')
                recipient_contact_new = map(lambda x: etree.fromstring(x), 
                                        pickle.loads(message_info[6]))[0]
                sender_contact_new = map(lambda x: etree.fromstring(x), 
                                        pickle.loads(message_info[7]))[0]
                author_new_list = map(lambda x: etree.fromstring(x), pickle.loads(message_info[8]))

                # Найдем, куда записать сохраненные значения в создаваемом ДЕЛО сообщении
                
                sender_contact = result_tree.xpath('*/sev:Sender/sev:Contact', 
                                                    namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0]

                recipient_contact = result_tree.xpath('*/sev:Recipient/sev:Contact', 
                                                    namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0]
                sender_contact.getparent().replace(sender_contact, sender_contact_new)
                recipient_contact.getparent().replace(recipient_contact, recipient_contact_new)

                if not self._is_acknowledgment:
                    # Уведомление
                    authors = result_tree.xpath('*/sev:Document/sev:Author', namespaces = {'sev': "http://www.eos.ru/2010/sev"})

                    if self._is_notification_confirm:
                        document = result_tree.xpath('.//sev:DocumentList/sev:Document', 
                                            namespaces = {'sev': "http://www.eos.ru/2010/sev"})[0]

                        authors = result_tree.xpath('*/sev:Document/sev:Author', namespaces = {'sev': "http://www.eos.ru/2010/sev"})

                        notification_datetime = tree.xpath('.//xdms:notification/xdms:documentAccepted/xdms:time', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
                        datetime_with_timezone = dateutil_parser.parse(notification_datetime).replace(tzinfo=pytz.timezone('Etc/GMT-3')).isoformat()

                        for author in author_new_list:
                            document.append(author)
                        attrs = {   'DATETIME' : datetime_with_timezone.encode('utf-8'),
                                    'RETURN_UID' : return_id.encode('utf-8'),
                                    'DOCUMENT_UID' : document_uid.encode('utf-8'),
                                    'DOCUMENT_GROUP' : document_group
                                }
                    if self._is_notification_refuse:
                        #notification_failure = tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:reason', 
                        #                                     namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
                        notification_datetime = tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:time', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text

                        notification_correspondent_name = tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:foundation/xdms:organization', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text

                        notification_reg_number = tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:foundation/xdms:num/xdms:number', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
                        notification_reg_date = tree.xpath('.//xdms:notification/xdms:documentRefused/xdms:foundation/xdms:num/xdms:date', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text

                        datetime_with_timezone = dateutil_parser.parse(notification_datetime).replace(tzinfo=pytz.timezone('Etc/GMT-3')).isoformat()

                        attrs = {   'DATETIME' : datetime_with_timezone.encode('utf-8'),
                                    'RETURN_UID' : return_id.encode('utf-8'),
                                    'DOCUMENT_UID' : document_uid.encode('utf-8'),
                                    'DOCUMENT_GROUP' : document_group
                                }

                else:
                    # Квитанция
                    # Определим значение дополнительных атрибутов, которые будут подставлены в Дело уведомление
                    # Дата и время формирования сообщения

                    acknowledgment_datetime = tree.xpath('.//xdms:acknowledgment/xdms:time', 
                                              namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
                    #acknowledgment_datetime = acknowledgment_datetime
                    datetime_with_timezone = dateutil_parser.parse(acknowledgment_datetime).replace(tzinfo=pytz.timezone('Etc/GMT-3')).isoformat()

                    acknowledgment_number = tree.xpath('.//xdms:acknowledgment/@xdms:uid', 
                                            namespaces = {'xdms':"http://www.infpres.com/IEDMS"})[0]

                    attrs = {   'REG_DATE' : document_reg_date,
                                'REG_NUMBER' : document_reg_number,
                                'DATETIME' : datetime_with_timezone,
                                'RETURN_UID' : return_id,
                                'DOCUMENT_UID' : document_uid,
                                'DOCUMENT_GROUP' : document_group
                            }

            else: 
                self._logger._log("Notification/Acknowledgment transport_guid: " + transport_guid[0] + " wasn't proccessed. Cause: (no Transport GIUD found in database)")

                return 0
        else: 
            transport_guid = tree.xpath('.//xdms:header/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})
            government_guid = tree.xpath('.//xdms:header/xdms:source/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})

            organization_name = tree.xpath('.//xdms:document/xdms:addressees/xdms:addressee/xdms:organization', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
            organization_name = unicode(organization_name)
            organization_uid = self._message_broker.get_address_uid(organization_name)
            organization_deloname = self._message_broker.get_address_deloname(organization_name)
            organization_docgroup = self._message_broker.get_address_docgroup(organization_name)

            if organization_uid == None:
                return None
            # Документ
            # Определим значение дополнительных атрибутов, которые будут подставлены в МЭДО сообщение
            attrs = {   'DOCUMENT_UID': str(uuid.uuid4()).replace('-',''),
                        'RETURN_UID': str(uuid.uuid4()).replace('-',''),
                        'TRANSPORT_UID': str(uuid.uuid4()).replace('-',''),
                        'TIME': datetime.now().replace(microsecond=0).isoformat(),
                        'FILE_UID': str(uuid.uuid4()).replace('-',''),
                        'ORGANIZATION_UID': organization_uid.encode('utf-8'),
                        'ORGANIZATION_DELONAME': organization_deloname.encode('utf-8'),
                        'ORGANIZATION_DOCGROUP': organization_docgroup.encode('utf-8')
                        }

        result_tree = etree.fromstring(
                                        Template(
                                            etree.tostring(result_tree, encoding='utf-8')
                                        ).substitute(attrs)).getroottree()
        # Загрузим XSD схему для проверки ДЕЛО сообщения

        xmlschema_doc = etree.parse(self._xsd_filename)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        # Проверим ДЕЛО сообщение по XSD схеме
        if xmlschema.validate(result_tree):
            # Проверка прошла успешно, можно сохранять сообщение
            self._transport_guid = attrs['RETURN_UID']
            self._delo_message_tree = result_tree

            # Если установлена опция формирования квитанций о получении пакета демоном, то создадим эту квитанцию
            if self._is_document:
                if self._message_broker._is_custom_medo_acknowledgment:
                    self._custom_ack_result_tree = self._create_custom_medo_acknowledgment()
                xdms_uid = tree.xpath('/xdms:communication/xdms:header/@xdms:uid', namespaces = {'xdms': "http://www.infpres.com/IEDMS"})[0]
                xdms_id = tree.xpath('/xdms:communication/xdms:document/@xdms:id', namespaces = {'xdms': "http://www.infpres.com/IEDMS"})[0]
                document_uid = tree.xpath('/xdms:communication/xdms:document/@xdms:uid', namespaces = {'xdms': "http://www.infpres.com/IEDMS"})[0]
                remote_reg_number = tree.xpath('.//xdms:document/xdms:num/xdms:number', namespaces = {'xdms': "http://www.infpres.com/IEDMS"})[0].text
                kwargs = {  'return_id': xdms_uid,
                            'document_id': xdms_id,
                            'remote_reg_number': remote_reg_number
                            }
                self._message_broker.set_message_info(self._transport_guid, "medo", **kwargs)
            else:
                if not self._is_acknowledgment:
                    if self._is_notification_refuse != None and self._is_notification_refuse != False:
                        self._logger._log('Warning! Registration refused. Packet GUID: ' + self._transport_guid + ' . Correspondent name: ' + notification_correspondent_name.encode('utf-8') + ' . Registration number: ' + notification_reg_number.encode('utf-8') + ' . Registration date: ' + notification_reg_date.encode('utf-8'), 'Daemon message. Registration Refused.')
                
            return u'Success'
        else:
            # Проверка не прошла, выведем причину непрохождения
            try:
                xmlschema.assertValid(result_tree)
            except Exception, e:
                self._logger._log(e.message) # LOG

            return None

    def _create_custom_medo_acknowledgment(self):
        # Создадим функцию преобразования
        # Загрузим файл Документа/Квитанции в формате МЭДО
        tree = etree.parse(self._xml_filename)
        # Загрузим шаблон преобразования Документа/Квитанции из дормата ДЕЛО в формат МЭДО
        xslt = etree.parse(self._xslt_template_filename_for_custom_acknowledgment)
        # Создадим функцию преобразования
        transform = etree.XSLT(xslt)
        # Получим преобразованный Документ/Квитанцию в формате ДЕЛО
        result_tree = transform(tree)

        source_organization_name = tree.xpath('.//xdms:document/xdms:addressees/xdms:addressee/xdms:organization', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0].text
        source_organization_name = unicode(source_organization_name)
        source_organization_uid = self._message_broker.get_address_uid(source_organization_name)
        document_uid = tree.xpath('.//xdms:header/@xdms:uid', namespaces={'xdms':"http://www.infpres.com/IEDMS"})[0]

        if source_organization_uid == None:
            return None
        # Документ
        # Определим значение дополнительных атрибутов, которые будут подставлены в МЭДО сообщение

        self._custom_ack_transport_guid = str(uuid.uuid4())

        attrs = {   'MESSAGE_UID': self._custom_ack_transport_guid,
                    'DATETIME': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                    'SOURCE_GUID': source_organization_uid.encode('cp1251'),
                    'ORGANIZATION_NAME': source_organization_name.encode('cp1251'),
                    'DOCUMENT_UID': document_uid.encode('cp1251')
                    }
        
        result_tree = etree.fromstring(
                                        Template(
                                            etree.tostring(result_tree, encoding='utf-8')
                                        ).substitute(attrs)).getroottree()

        # Загрузим XSD схему для проверки ДЕЛО сообщения

        xmlschema_doc = etree.parse(self._xsd_acknowledgment_filename)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        # Проверим ДЕЛО сообщение по XSD схеме
        if xmlschema.validate(result_tree):
            return result_tree
        else:
            # Проверка не прошла, выведем причину непрохождения
            try:
                xmlschema.assertValid(result_tree)
            except Exception, e:
                print etree.tostring(result_tree,encoding='utf-8')
                self._logger._log(e.message) # LOG

            return None


    def receive(self):
        result = self._create_message()
        if result:
            self._create_delo_folders()
            # Скопируем в директорию МЭДО сообщения приложенные файлы ДЕЛО сообщения
            self._copy_applied_files()
            # Сохраним сообщение в директорию отправки ДЕЛО
            self._save_delo_message()
            # Создадим в директории МЭДО сообщения файл envelope.ini
            self._create_env()

            if self._is_document:
                if self._message_broker._is_custom_medo_acknowledgment:
                    self._save_custom_medo_acknowledgment()
                    self._save_custom_medo_envelope()
            # Переместим сообщениябщение МЭДО в архивнут папку, чтобы не отправлять его еще раз
            self._move_message_to_archive()


class MessageBroker(object):
    _is_debug_mode = False
    _is_subdir_date = False
    _DIRS = {
        'SEND_DIR' : 'D:\SEVDELO\Transport\Archive\Out', # Дело сообщения и Квитанции на отправку #Transport\Archive\Out
        'RECEIVE_DIR' : 'Z:\MEDO_OUT', # МЭДО сообщения и квитанции для приема 
        'ARCHIVE_DIR' : 'archive', # Архив ДЕЛО сообщений, которые УСПЕШНО отправлены
        'ARCHIVE_MEDO_DIR' : 'archive_medo', # Архив МЭДО сообщений, которые УСПЕШНО? приняты
        'MEDO_SEND_DIR' : 'Z:\MEDO_IN', # МЭДО сообщения, полученные из наших ДЕЛО сообщений. Их нужно отправить.
        'DELO_RECEIVE_DIR' : 'D:\SEVDELO\DocumentIn', # ДЕЛО сообщения, полученные из их МЭДО сообщений. Их нужно загрузить в ДЕЛО.
        'DELO_RECEIVE_DIR_REPORT' : 'D:\SEVDELO\ReportIn', 
        'TEMPLATES_DIR' : 'templates' # Шаблоны
    }
    _JSON_FILENAME = 'db.json'
    _SQLITE_FILENAME = 'xfers.sqlite'
    _sqlite_connection = ''
    _logger = ''
    _config_parser = ''
    def __init__(self):
        self._config_parser = ConfParser()
        if self._config_parser:
            self._is_debug_mode = self._config_parser._config.getboolean('MessageBroker', 'is_debug_mode')
            self._is_subdir_date = self._config_parser._config.getboolean('MessageBroker', 'is_subdir_date')
            self._is_custom_medo_acknowledgment = self._config_parser._config.getboolean('MessageBroker','is_custom_medo_acknowledgment')
            self._JSON_FILENAME = self._config_parser._config.get('MessageBroker', 'json_filename')
            self._SQLITE_FILENAME = self._config_parser._config.get('MessageBroker', 'sqlite_filename')

            if self._is_debug_mode:
                self._DIRS = {
                    'SEND_DIR' : self._config_parser._config.get('MessageBroker', 'debug_send_dir'), # Дело сообщения и Квитанции на отправку #Transport\Archive\Out
                    'RECEIVE_DIR' : self._config_parser._config.get('MessageBroker', 'debug_receive_dir'), # МЭДО сообщения и квитанции для приема 
                    'ARCHIVE_DIR' : self._config_parser._config.get('MessageBroker', 'debug_archive_dir'), # Архив ДЕЛО сообщений, которые УСПЕШНО отправлены
                    'ARCHIVE_MEDO_DIR' : self._config_parser._config.get('MessageBroker', 'debug_archive_medo_dir'), # Архив МЭДО сообщений, которые УСПЕШНО? приняты
                    'MEDO_SEND_DIR' : self._config_parser._config.get('MessageBroker', 'debug_medo_send_dir'), # МЭДО сообщения, полученные из наших ДЕЛО сообщений. Их нужно отправить.
                    'DELO_RECEIVE_DIR' : self._config_parser._config.get('MessageBroker', 'debug_delo_receive_dir'), # ДЕЛО сообщения, полученные из их МЭДО сообщений. Их нужно загрузить в ДЕЛО.
                    'DELO_RECEIVE_DIR_REPORT' : self._config_parser._config.get('MessageBroker', 'debug_delo_receive_dir_report'), 
                    'TEMPLATES_DIR' : self._config_parser._config.get('MessageBroker', 'templates_dir')
                }
            else:
                self._DIRS = {
                        'SEND_DIR' : self._config_parser._config.get('MessageBroker', 'send_dir'), # Дело сообщения и Квитанции на отправку #Transport\Archive\Out
                        'RECEIVE_DIR' : self._config_parser._config.get('MessageBroker', 'receive_dir'), # МЭДО сообщения и квитанции для приема 
                        'ARCHIVE_DIR' : self._config_parser._config.get('MessageBroker', 'archive_dir'), # Архив ДЕЛО сообщений, которые УСПЕШНО отправлены
                        'ARCHIVE_MEDO_DIR' : self._config_parser._config.get('MessageBroker', 'archive_medo_dir'), # Архив МЭДО сообщений, которые УСПЕШНО? приняты
                        'MEDO_SEND_DIR' : self._config_parser._config.get('MessageBroker', 'medo_send_dir'), # МЭДО сообщения, полученные из наших ДЕЛО сообщений. Их нужно отправить.
                        'DELO_RECEIVE_DIR' : self._config_parser._config.get('MessageBroker', 'delo_receive_dir'), # ДЕЛО сообщения, полученные из их МЭДО сообщений. Их нужно загрузить в ДЕЛО.
                        'DELO_RECEIVE_DIR_REPORT' : self._config_parser._config.get('MessageBroker', 'delo_receive_dir_report'), 
                        'TEMPLATES_DIR' : self._config_parser._config.get('MessageBroker', 'templates_dir')
                    }
        else:
            if self._is_debug_mode:
                self._DIRS = {
                        'SEND_DIR' : 'send', # Дело сообщения и Квитанции на отправку #Transport\Archive\Out
                        'RECEIVE_DIR' : 'receive', # МЭДО сообщения и квитанции для приема 
                        'ARCHIVE_DIR' : 'archive', # Архив ДЕЛО сообщений, которые УСПЕШНО отправлены
                        'ARCHIVE_MEDO_DIR' : 'archive_medo', # Архив МЭДО сообщений, которые УСПЕШНО? приняты
                        'MEDO_SEND_DIR' : 'medo_send', # МЭДО сообщения, полученные из наших ДЕЛО сообщений. Их нужно отправить.
                        'DELO_RECEIVE_DIR' : 'delo_receive', # ДЕЛО сообщения, полученные из их МЭДО сообщений. Их нужно загрузить в ДЕЛО.
                        'DELO_RECEIVE_DIR_REPORT' : 'delo_receive_dir_report', 
                        'TEMPLATES_DIR' : 'templates' # Шаблоны
                    }
            else:
                self._DIRS = {
                        'SEND_DIR' : 'D:\SEVDELO\Transport\Archive\Out', # Дело сообщения и Квитанции на отправку #Transport\Archive\Out
                        'RECEIVE_DIR' : 'Z:\MEDO_OUT', # МЭДО сообщения и квитанции для приема 
                        'ARCHIVE_DIR' : 'archive', # Архив ДЕЛО сообщений, которые УСПЕШНО отправлены
                        'ARCHIVE_MEDO_DIR' : 'archive_medo', # Архив МЭДО сообщений, которые УСПЕШНО? приняты
                        'MEDO_SEND_DIR' : 'Z:\MEDO_IN', # МЭДО сообщения, полученные из наших ДЕЛО сообщений. Их нужно отправить.
                        'DELO_RECEIVE_DIR' : 'D:\SEVDELO\DocumentIn', # ДЕЛО сообщения, полученные из их МЭДО сообщений. Их нужно загрузить в ДЕЛО.
                        'DELO_RECEIVE_DIR_REPORT' : 'D:\SEVDELO\ReportIn', 
                        'TEMPLATES_DIR' : 'templates' # Шаблоны
                    }

        self._logger = Logger()

    def get_address_uid(self, transport_uid):
        #transport_uid.encode('utf-8')
        with open (self._JSON_FILENAME, "r") as address_file:
            data = json.load(address_file)
        try:
            if (data[transport_uid]):
                return data[transport_uid]['delo_id']
            else:
                return None
        except Exception, e:
            print "Recipient: " + transport_uid
            self._logger._log(u"Error: No recipient organization UID found in json database. Organization name mismatch")
            return None

    def get_address_deloname(self, transport_uid):
        with open (self._JSON_FILENAME, "r") as address_file:
            data = json.load(address_file)
        try:
            if (data[transport_uid]):
                return data[transport_uid]['delo_name']
            else:
                return None
        except Exception, e:
            print "Recipient: " + transport_uid
            self._logger._log(u"Error: No recipient organization UID found in json database. Organization name mismatch")
            return None

    def get_address_docgroup(self, transport_uid):
        with open (self._JSON_FILENAME, "r") as address_file:
            data = json.load(address_file)
        try:
            if (data[transport_uid]):
                return data[transport_uid]['doc_group']
            else:
                return None
        except Exception, e:
            print "Recipient: " + transport_uid
            self._logger._log(u"Error: No recipient organization UID found in json database. Organization name mismatch")
            return None

    def set_message_info(self, transport_guid, document_type, **kwargs):
        c = self._sqlite_connection.cursor()
        if document_type == "delo":
            c.execute('''insert into xfers values (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)''', (   
                        transport_guid, 
                        kwargs['return_id'], 
                        kwargs['registration_number'], 
                        kwargs['registration_date'], 
                        kwargs['document_uid'], 
                        kwargs['sender_contact_pickle'], 
                        kwargs['recipient_contact_pickle'], 
                        kwargs['author_pickle'],
                        kwargs['document_group']))
        else:
            c.execute('''insert into xfers values (NULL, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, NULL, ?)''', (
                        transport_guid,
                        kwargs['return_id'], 
                        kwargs['document_id'],
                        kwargs['remote_reg_number']))

        self._sqlite_connection.commit()

    def get_message_info(self, transport_guid):
        c = self._sqlite_connection.cursor()

        c.execute('''select * from xfers where transport_uid=?''', (transport_guid,))
        return c.fetchone()

    def get_message_info_by_document_uid(self, document_uid):
        c = self._sqlite_connection.cursor()

        c.execute('''select * from xfers where document_uid=?''', (document_uid,))
        return c.fetchone()        

    def _process_send_folder(self):
        "Обработка директории отправляемых писем ДЕЛО" 
        message_uid_list = []
        failure = ""
        # Просмотрим все имена в директории отправленных писем ДЕЛО
        for filename in os.listdir(self._DIRS['SEND_DIR']):
            for subfilename in  os.listdir(os.path.join(self._DIRS['SEND_DIR'], filename)):
                applied_files_count = 0
                is_report = False
                # Переименуем файлы вложений. Уберем служебные индексы [1],[2], ... прикрепленные системой Дело.
                if os.path.isdir(os.path.join(os.path.join(self._DIRS['SEND_DIR'], filename), subfilename)):
                    for insidefilename in os.listdir(os.path.join(os.path.join(self._DIRS['SEND_DIR'], filename), subfilename)):
                        applied_files_count = applied_files_count + 1
                        if "[" in insidefilename and "]" in insidefilename:
                            path = os.path.join(os.path.join(os.path.join(self._DIRS['SEND_DIR'], filename), subfilename),insidefilename)
                            path_new = re.sub('\[\d+\]','', path)
                            if path_new:
                                os.rename(path , path_new)
                        if insidefilename.endswith(".pdf.pdf") or insidefilename.endswith(".PDF.pdf"):
                            failure = "One or more applied files have a bad extension (.pdf.pdf) " + insidefilename
                        if insidefilename == "Report.xml":
                            is_report = True
                    if not is_report and applied_files_count <= 1:
                        failure = "There're no files applied. Nothing to send."
                
                # Выберем файлы (не директории), имена которых оканчиваются на .env
                if os.path.isfile(os.path.join(os.path.join(self._DIRS['SEND_DIR'], filename), subfilename)) and subfilename.endswith('.env'):
                    if failure != "":
                        self._logger._log('Outbound file: ' + subfilename + ' Error: \'' + failure + '\'.', 'Daemon message. Error.')
                    else:
                        # Отправим сообщения
                        self._logger._log('Outbound file: \'' + subfilename + '\' processing begins.')
                        MedoMesage(message_uid=subfilename.split('.')[0], message_broker=self, current_process_dir=os.path.join(self._DIRS['SEND_DIR'],filename)).send()

    def _process_receive_folder(self):
        "Обработка директории входящих писем из МЭДО"
        # Просмотрим все имена в директории входящих писем МЭДО
        for filename in os.listdir(self._DIRS['RECEIVE_DIR']):
            # Выберем директории (их названия и есть message_uid)
            if os.path.isdir(os.path.join(self._DIRS['RECEIVE_DIR'], filename)):
                # Примем сообщения
                for attached_filename in os.listdir(os.path.join(self._DIRS['RECEIVE_DIR'], filename)):
                    if ".doc.doc" in attached_filename or ".pdf.pdf" in attached_filename or ".tif.tif" in attached_filename or ".txt.txt" in attached_filename:
                        path = os.path.join(os.path.join(self._DIRS['RECEIVE_DIR'], filename),attached_filename)
                        path_new = path[:-4]
                        os.rename(path, path_new)
                self._logger._log('Inbound file: \'' + filename +'\' processing begins.')
                DELOMessage(message_uid=filename, message_broker=self).receive()

    def _check_receive_folder(self):
        try:
            if not os.path.exists(self._DIRS['RECEIVE_DIR']):
                raise Exception(u"Access Error (no access to receive directory).")
        except Exception,e:
            self._logger._log(e.message)
            return False
        return True

    def _check_send_folder(self):
        try:
            if not os.path.exists(self._DIRS['SEND_DIR']):
                raise Exception(u"Access Error (no access to send directory).")
        except Exception,e:
            self._logger._log(e.message)
            return False
        return True
    
    def run(self):
        self._sqlite_connection = sqlite3.connect(self._SQLITE_FILENAME)
        if self._sqlite_connection:
            if self._check_receive_folder():
                self._process_receive_folder()
            if self._check_send_folder():
                self._process_send_folder() 
            self._sqlite_connection.close()
        else:
            self._logger._log('Error. Cannot connect to database ' . self._SQLITE_FILENAME)

class Logger:
    _log_filename = 'log.txt'
    _log_file_path = ""
    _DIRS = {'LOG_DIR' : 'log'}
    _e_str = ""
    _mailer = ""
    _is_mailer = True
    _config_parser = ""
    def __init__(self):
        self._config_parser = ConfParser()
        if self._config_parser:
            self._log_filename = self._config_parser._config.get('Logger','log_filename')
            self._DIRS = {'LOG_DIR': self._config_parser._config.get('Logger', 'log_dir')}
            self._is_mailer = self._config_parser._config.getboolean('Logger', 'is_mailer')

        self._log_file_path = os.path.join(self._DIRS['LOG_DIR'], self._log_filename)

    def _log(self, error_string, subject=None):
        with open(os.path.join(self._DIRS['LOG_DIR'], 'log.txt'), 'a') as f:
            self._e_str = datetime.now().strftime('%y-%m-%d %H:%M:%S') + ": " + error_string + '\n'
            f.write(self._e_str)
            self._plog()
            if self._is_mailer:
                if subject != "" and subject != None: 
                    self._mlog(subject)
                else:
                    self._mlog('Daemon message')

    def _plog(self):
        print self._e_str

    def _mlog(self, subject):
        if self._is_mailer:
            self._mailer = Mailer(self._e_str, subject)
            self._mailer._send_mail()

class Mailer:
    _server_host = 'server-v-e.admgor.local'
    _sender_address = 'delo-daemon@vologda-city.ru'
    _mail_subject = ''
    _mail_text = ''
    _msg = ''
    _connection = ''
    _config_parser = ''
    _RECIPIENTS = {
        'belov.artem@vologda-city.ru',
        'sedova.olga@vologda-city.ru'
    }
    def __init__(self, mailtext, mailsubject):
        self._config_parser = ConfParser()
        if self._config_parser:
            self._server_host = self._config_parser._config.get('Mailer','server_host')
            self._server_address = self._config_parser._config.get('Mailer','server_address')
            self._RECIPIENTS = self._config_parser._config.items('Recipients')
        self._mail_text = mailtext
        self._mail_subject = mailsubject

    def _send_mail(self):
        try:
            self._connection = smtplib.SMTP(self._server_host)
        except Exception, e:
            print e.message
        if self._connection:
            for address in self._RECIPIENTS:
                self._msg = MIMEText(self._mail_text)
                self._msg['Subject'] = self._mail_subject
                self._msg['From'] = self._sender_address
                if self._config_parser:
                    self._msg['To'] = address[1]
                    self._connection.sendmail(self._sender_address, [address[1]], self._msg.as_string())
                else:
                    self._msg['To'] = address
                    self._connection.sendmail(self._sender_address, [address], self._msg.as_string())

            self._connection.quit()
        else:
            print "Error: Cannot connect to Mail server"

class ConfParser:
    _config_file = 'config.ini'
    _config = ''
    
    def __init__(self):
        self._read_inifile()

    def _read_inifile (self):
        self._config = ConfigParser.RawConfigParser()
        self._config.read(self._config_file)

    def _create_section (self, section_name):
        self._config.add_section(section_name)

    def _write_to_inifile (self, section_name, parameter, value):
        self._config.set(section_name, parameter, value)
        with open (self._config,'wb') as configfile:
            self._config.write(configfile)

class Daemon:
    _time_interval = 60
    _message_broker = ""
    _config_parser = ""
    def __init__(self):
        self._config_parser = ConfParser()
        if self._config_parser:
            self._time_interval = self._config_parser._config.getint('Daemon','time_interval')

        self._message_broker = MessageBroker()

    def run(self):
        # Запуск Демона
        while True:
            print "Proccessing started: " + datetime.now().strftime('%y-%m-%d %H:%M:%S')
            self._message_broker.run()
            time.sleep(self._time_interval)

Daemon().run()
