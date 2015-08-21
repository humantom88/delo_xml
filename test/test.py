# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText

# Open a plain text file for reading.  For this example, assume that
# the text file contains only ASCII characters.
fp = open(textfile, 'rb')
# Create a text/plain message
msg = MIMEText(fp.read())
fp.close()

# me == the sender's email address
# you == the recipient's email address
msg['Subject'] = 'The contents of %s' % textfile
msg['From'] = me
msg['To'] = you

# Send the message via our own SMTP server, but don't include the
# envelope header.
s = smtplib.SMTP('localhost')
s.sendmail(me, [you], msg.as_string())
s.quit()

# from lxml import etree

# import sqlite3

# conn = sqlite3.connect('xfers.db')
# c = conn.cursor()
# # c.execute('''DROP TABLE xfers''')
# # c.execute('''CREATE TABLE xfers 
# #          (  id INTEGER PRIMARY KEY AUTOINCREMENT,
# #             transport_uid text,
# #             return_id text,
# #             registration_number text,
# #             registration_date text,
# #             document_uid text
# #             sender_contact_pickle text, 
# #             recipient_contact_pickle text,
# #             author_pickle text
# #             )''')
# c.execute('''select * from xfers''')
# a = c.fetchall()
# c.execute('''INSERT INTO xfers VALUES (NULL,'b','c','d','e','f','g','h')''')
# conn.commit()

# conn.close()


def main():

    # tree = etree.parse('1.xml')
    # xslt = etree.parse('1.xslt')
    # transform = etree.XSLT(xslt)
    # result_tree = transform(tree)

    # print etree.tostring(
    #         etree.fromstring(
    #                         etree.tostring(result_tree, encoding='utf-8'),
    #                         parser=etree.XMLParser(recover=True)),
    #         encoding='utf-8')

    # with open('1.out', 'w') as f:
    #     doc = etree.ElementTree(result_tree.getroot())
    #     doc.write(f, encoding='utf-8', pretty_print=True)
    #     #f.write(etree.tostring(result_tree, encoding='utf-8'))

main()