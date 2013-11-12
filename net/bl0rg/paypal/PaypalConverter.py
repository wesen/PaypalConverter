# -*- coding: utf-8 -*-
'''
Created on Dec 29, 2011

@author: manuel
'''

import csv
import codecs
import cmd
import os
from operator import itemgetter, attrgetter
import time
import commands
import re
import cStringIO
import readline
import traceback
import sys
import email
from email.header import decode_header

def utf8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_encoder(unicode_csv_data), dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        # data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        """
        rows:
        """
        for row in rows:
            self.writerow(row)

IMPORTANT_FIELDS = [u"Datum", u"Name", u"Status", u"Art", u"Von", u"An", "uWährung", u"Brutto", u"Gebühr", u"Netto", u"Guthaben", u"Transaktionscode", u"Txn-Referenzkennung"] 

class Txn:
    def __init__(self, auszug, row):
        self.auszug = auszug
        self.date = time.strptime(row[u"Datum"], "%d.%m.%Y")
        self.name = row[u"Name"]
        self.status = row[u"Status"]
        self.art = row[u"Art"]
        def price_repl(match):
            if match.group(0) == u".":
                return ","
            else:
                return "."
        def conv_price(str):
            return re.sub("[,.]", price_repl, str)
        
        def conv_waehrung(waehrung):
            WAEHRUNG = {
                "USD": u"USD"
            #    "EUR": u"\u20ac",
            #    "GBP": u"\u00a3"
            }
            if WAEHRUNG.has_key(waehrung):
                return WAEHRUNG[waehrung]
            else:
                return waehrung
            
        self.waehrung = conv_waehrung(row[u"Währung"])
            
        self.brutto = conv_price(row[u"Brutto"])
        self.gebuehr = conv_price(row[u"Gebühr"])
        self.netto = conv_price(row[u"Netto"])
        self.guthaben = conv_price(row[u"Guthaben"])
        self.id = row[u"Transaktionscode"]
        self.ref = row[u"Txn-Referenzkennung"]
        self.sender = row[u"Von E-Mail-Adresse"]
        self.receiver = row[u"An E-Mail-Adresse"]
        self.description = row[u"Verwendungszweck"]
        self.real_brutto = ""

    def getRef(self):
        return self.auszug.getTxn(self.ref)

    def getReferrers(self):
        return self.auszug.getReferringTxns(self.id)
    
    def __repr__(self):
        return (u"<Txn: %s (%s - \"%s\") - %s - \"%s\" %s %s - %s>" % (self.getTypeCharacter(), self.id, self.art, time.strftime(u"%d.%m.%Y", self.date), self.name, self.waehrung, self.brutto, self.description)).encode('ascii', 'ignore')

    def getTypeCharacter(self):
        if self.isPaypalTxn():
            return "P"
        elif self.isConversion():
            return "C"
        elif self.isAuthorization():
            return "A"
        elif self.isRecurring():
            return "R"
        else:
            return "E"
        
    def getCurrencyConversion(self, destWaehrung):
        conversions = [ x for x in self.getReferrers() if x.isConversion() ]
        _from = [ x for x in conversions if x.waehrung != destWaehrung ]
        _to = [ x for x in conversions if x.waehrung == destWaehrung ]
        if _from and _to:
            return (_from[0], _to[0])
        else:
            return (None, None)
        
    def convertCurrency(self):
        (_from, _to) = self.getCurrencyConversion("USD")
        waehrung = "USD"
        if not _to:
            waehrung = "EUR"
            (_from, _to) = self.getCurrencyConversion("EUR")
        if _to:
            self.real_brutto = "%s %s" % (self.waehrung, self.brutto)
            self.brutto = _to.brutto
            self.waehrung = waehrung
            self.guthaben = _to.guthaben
        
    def getUSDValue(self):
        (_from, _to) = self.getCurrencyConversion()
        if _to:
            return _to.brutto
        else:
            return None
        
    def needsConversion(self):
        return self.waehrung != "USD"
    
    def isPaypalTxn(self):
        return self.name == u"PayPal"
    
    def isConversion(self):
        return self.art == u"Währungsumrechnung"
    
    def isAuthorization(self):
        return self.art == u"Autorisierung"
    
    def isRecurring(self):
        return self.art == "Abonnementzahlung gesendet"
    
    def isPayment(self):
        return (not self.isPaypalTxn()) and (not self.isAuthorization()) and (not self.isConversion())
    
    def isPrivate(self):
        regexps = ["ZERO INCH", "Boomkat", "Bleep", "buyolympia", "Spotify"]
        for _re in regexps:
            if re.match(_re, self.name):
                return True
        return False

    def getMonth(self):
        return self.date.tm_mon

    def getYear(self):
        return self.date.tm_year

    
    def toCSV(self):
        row = {}
        row["date"] = time.strftime("%d.%m.%Y", self.date)
        row["name"] = self.name
        row["original"] = self.real_brutto
        row["brutto"] = u"%s %s" % (self.waehrung, self.brutto)
        if self.gebuehr != u"0.00":
            row["gebuehr"] = u"%s %s" % (self.waehrung, self.gebuehr)
        else:
            row["gebuehr"] = u""
        row["guthaben"] = u"%s %s" % (self.waehrung, self.guthaben)
        row["notes"] = u""
        if self.isPrivate():
            row["notes"] += u"PRIVAT"
        else:
            row["notes"] += self.description
        #if self.findEmail():
        #    row["notes"] += u" EMAIL"
            
        return row
    
    def findSimilarEmails(self):
        _time = time.strftime("%d/%m/%Y", self.date)
        _created = "created:%s" % _time
        def find(query):
            query = query.encode('ascii', 'ignore')
#            print "searching for \"%s\" on %s" % (query,_time)
            return "mdfind -interpret '%s \"%s\" kind:mail'" % (_created, query)

        if self.isConversion() or self.isAuthorization():
            return []

        ignores = [ "von Euro", "in Euro", "von US-Dollar", "in US-Dollar", "Kreditkarte", "Bankkonto (Lastschrift)"]
        _cmd = ";".join([find(x) for x in [self.name, self.receiver] if x not in ignores])
        if _cmd == "":
            return []
#        print _cmd.encode('ascii', 'ignore')
        _file = commands.getstatusoutput(_cmd.encode('ascii', 'ignore'))[1]
        files = _file.split("\n")
        _email = self.findEmail()
        if _email and files.count(_email) == 0:
            files.append(_email)
#        if files.count(_email) > 0:
#            files.remove(_email)
        return [ f for f in files if f ]
    
    def openSimilarEmails(self):
        for email in self.findSimilarEmails():
            commands.getstatusoutput("open \"%s\"" % email)
    
    def findEmail(self):
        if self.isConversion() or self.isAuthorization():
            return None

#        print "searching for id: %s" % self.id
        _cmd = "/usr/local/bin/sense -n 0.8 'mdfind -interpret \"\"%s\" kind:mail\"'" % self.id
#        print _cmd.encode('ascii', 'ignore')
        _file = commands.getstatusoutput(_cmd)[1]
        if _file != '':
#            print "found %s" % _file
            return _file
        else:
            return None

    def parseKmdOutput(self, path):
        "Run Spotlight 'mdls' command on a file and return result as a Python dict."
        # needs special handling of date fields like "2007-05-24 13:08:05 +0200"

        # run 'mdls' command, grab its output and remove newlines
        output = commands.getoutput(str("mdls " + path))
        s = output.replace('\n', '')

        # split the string on 'kMD' into lines
        kmdLines = s.split('kMD')
        kmdLines = [line for line in kmdLines if line]
        # print "** kmdLines"
        # pp(kmdLines)
        items = [re.match("(\w+) *= *(.*)", line) for line in kmdLines]
        items = [m.groups() for m in items if m]
        # print "** items"
        # pp(items)

        # reinsert kMD
        items = [("kMD" + k, v) for (k, v) in items]
        # print "** items"
        # pp(items)

        # work around a feature in Mac OS X
        # (which puts no quotes around 'simple' strings)
        pairs = []
        for item in items:
            try:
                k, v = item
            except:
                # print "### item", item
                # print path
                raise
            v = v.strip()
            try:
                ev = eval(v)
            except:
                # print "### failed evaluating", repr(v)
                if v[0] not in ('"', "'"):
                    v = v.replace('"', '')
                    v = v.replace("'", '')
                if v.startswith('('): # and v.startswith(')'):
                    # print "---", v
                    v = re.sub("(\w+)", lambda m:'"%s"' % m.groups()[0], v)
                    # also turn a single name into a one element tuple
                    # print "****", repr(v)
                    # if type(eval(v)) == str:
                    #     v = "(%s,)" % v
                # print "***", repr(v)
            try:
                pairs.append([k, eval(v)])
            except SyntaxError:
                pairs.append([k, v])

        data = dict(pairs)

        return data

    def getEmailInfo(self, info):
        f = open(info)
        data = f.read()
        data = data.split('\n', 1)[1]
        msg = email.message_from_string(data)
        _from = decode_header(msg.get("From"))
        if len(_from) > 0:
            _from = _from[0][0]
        subject = decode_header(msg.get("Subject"))
        if len(subject) > 0:
            subject= subject[0][0]
        if _from and subject:
            return "Subject: %s from: %s" % (subject, _from)
#        data = self.parseKmdOutput(info)
#        print "email %s info: %s" % (info, len(data.keys()))
#        if len(data) > 0:
#            return "Subject: %s from: %s" % (data.get("kMDItemDisplayName", ""), data.get("kMDItemAuthors", "Unknown"))
        else:
            return None

    def openEmail(self):
        email = self.findEmail()
        if email:
            commands.getstatusoutput("open \"%s\"" % email)
                
class Auszug:
    def __init__(self):
        self.txns = {}
        self.refTxns = {}

    def getReferringTxns(self, _id):
        return self.refTxns.get(_id, [])
    
    def getTxn(self, _id):
        return self.txns.get(_id)
    
    def addTxn(self, txn):
        if self.txns.has_key(txn.id):
            raise Exception('already read transaction', txn)
        self.txns[txn.id] = txn
        refs = self.refTxns.get(txn.ref)
        if refs:
            refs.append(txn)
        else:
            self.refTxns[txn.ref] = [txn]
        
    def sortedTxns(self):
        return [ self.txns[x] for x in sorted(self.txns, key=lambda x: self.txns[x].date, reverse=False)]
    
    def sortedPayments(self):
        return [ x for x in self.sortedTxns() if x.isPayment() ]
    
    def convertCurrency(self):
        for txn in self.txns.values():
            if not txn.isPayment():
                continue
            if txn.needsConversion():
                txn.convertCurrency()
    
    @staticmethod
    def readCSV(path):
        auszug = Auszug()
        reader = unicode_csv_reader(codecs.open(path, 'r', 'iso-8859-1'), delimiter=',', quotechar="\"")
        header = [ x.strip() for x in reader.next() ]
        for row in [Txn(auszug, dict([(field, value) for field, value in zip(header, row)])) for row in reader]:
            # print row.__repr__().encode('ascii', 'ignore')
            auszug.addTxn(row)
        auszug.convertCurrency()
        return auszug
    
    def openEmails(self):
        for txn in self.sortedTxns():
            if txn.findEmail() and (not txn.isPrivate()):
                print txn.__repr__().encode('ascii', 'ignore')
                txn.openEmail()
                raw_input("Press Enter to continue")
    
    
    def printCSV(self, path):
        """
        Foobar
        """
        start_month = None
        start_year = None
        newFile = True
        writer = None
        f = None
        for txn in self.sortedTxns():
            if txn.isPayment():
                row = txn.toCSV()
                print row
                if row["brutto"] != "":
                    if txn.getYear() != start_year:
                        start_year = txn.getYear()
                        newFile = True
                    if txn.getMonth() != start_month:
                        start_month = txn.getMonth()
                        newFile = True
                    if newFile:
                        newFile = False
                        if f:
                            f.close()
                            f = None
                        f = codecs.open("%s-%.4d-%.2d.csv" % (path, start_year, start_month), 'w', 'utf-8')
                        writer = UnicodeWriter(f)
                        #        writer = csv.writer(codecs.open(path, 'w', 'utf-8'), delimiter=',', quotechar="\"")
                        writer.writerow([u"Datum", u"Name", u"Brutto", u"Original", u"Gebühr", u"Kontostand", u"Notes"])

                    writer.writerow([row[x] for x in ["date", "name", "brutto", "original", "gebuehr", "guthaben", "notes"]])

def intTryParse(value):
    try:
        return int(value)
    except ValueError:
        return None

class PaypalConsole(cmd.Cmd):
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "paypal> "
        self.intro = "Welcome to the Paypal Converter"
        self.auszug = None
        # self.do_csv("/tmp/test.csv")
        # self.do_list(None)
        readline.set_completer_delims(' \t\n`@#$%^&*()-=+[{]}\\|;:\'",<>?')

    def do_csv(self, name):
        try:
            self.auszug = Auszug.readCSV(os.path.expanduser(name))
        except:
            traceback.print_exc(file=sys.stdout)
            print "Error parsing CSV"

    def do_out(self, name):
        if not self.auszug:
            print "please first parse a csv"
            return
        try:
            self.auszug.printCSV(name)
            print "written output csv to " + name
        except:
            traceback.print_exc(file=sys.stdout)
            print "error while writing csv"

    def do_list(self, args):
        for txn in self.auszug.sortedTxns():
            print txn.__repr__().encode('ascii', 'ignore')

    def do_emails(self, args):
        if not self.auszug:
            print "please first parse a csv"
            return
        try:
            for txn in self.auszug.sortedTxns():
                if not txn.isPayment():
                    continue
                if txn.isPrivate():
                    continue
                if txn.isConversion() or txn.isAuthorization():
                    continue
                if txn.name in ["Kreditkarte", "Bankkonto (Lastschrift)"]:
                    continue
                print txn.__repr__().encode('ascii', 'ignore')
                emails = txn.findSimilarEmails()
#                print "similar %s" % emails
                i = 0
                for f in emails:
                    i += 1
                    info = txn.getEmailInfo(f)
                    if info:
                        print "%s) %s" % (i, info)
                if len(emails) > 0:
                    stop = False
                    while not stop:
                        _open = raw_input("Open email: ")
                        idx = intTryParse(_open)
                        if idx is not None and 0 < idx <= len(emails):
                            commands.getstatusoutput("open \"%s\"" % emails[idx - 1])
                        elif _open == "":
                            stop = True
                print "\n"
        except Exception as e:
            print "error while searching for emails"
            traceback.print_exc(file=sys.stdout)
            print e

    def list_files(self, start, ends = None):
        res = []
        _real = os.path.expanduser(start)
        if os.path.isfile(_real):
            return [start]

        _dir = os.path.dirname(_real)
        if _dir == "":
            _dir = "."
            _base = ""
        else:
            _base = os.path.basename(_real)

        dir = os.path.dirname(start)
#        print dir
        if not (dir == "") and not dir.startswith(os.path.sep):
            dir += os.path.sep

#        print "_dir " + _dir + " dir " + dir

        for f in  os.listdir(_dir):
            full = _dir + os.path.sep + f
#            print full
#            print f.startswith(_base)
#            print _base
            if (os.path.isfile(full) and
                f.startswith(_base) and
                (not ends or f.endswith(ends))
                and (f != _base)):
#                print "adding f " + f + " base " + _base
                res.append(dir + f)
            elif os.path.isdir(full) and f.startswith(_base):
                res.append(dir + f + os.path.sep)
#        print "\n"
#        print  res
#        print "\n"
        return res

    def complete_csv(self, text, line, begidx, endidx):
        mline = line.split(' ', 1)[1]
#        print "mline " + mline
        if not mline or mline == "":
            return self.list_files(".", ".csv")
        else:
            return self.list_files(mline, ".csv")

    def help_csv(self):
        print '\n'.join(
            ["load csv",
            "Parse a paypal csv dump"])

    def do_EOF(self, args):
        return self.do_exit(args)

    def do_exit(self, args):
        return -1

if __name__ == '__main__':
    PaypalConsole().cmdloop()

#auszug = Auszug.readCSV("/Users/manuel/Downloads/Herunterladen.csv")
#auszug.printCSV("/tmp/out.csv")
#print "printed CSV out to /tmp/out.csv\n"
#for txn in auszug.sortedTxns():
#    email = txn.findEmail()
#    emails = txn.findSimilarEmails()
#    print "%s: %s, %s\n" % (txn.id, email, emails)
#
#def openSpotlight(query):
#    applescript = '''on run argv # we expect program arguments
#    tell application "Finder"
#        activate # focus Finder
#        tell application "System Events"
#            keystroke "f" using command down # press Cmd-F
#            keystroke (item 1 of argv) # enter the program argument into search box
#            key code 36 # press enter
#            key code 48 # press tab
#            keystroke " " # press space
#        end tell
#    end tell
#end run
#'''
#    _cmd = "osascript " + " ".join([ "-e '%s'" % x for x in applescript.split('\n')]) + " " + query
#    commands.getstatusoutput(_cmd)
#
#
