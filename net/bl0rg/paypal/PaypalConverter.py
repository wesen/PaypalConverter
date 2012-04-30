# -*- coding: utf-8 -*-
'''
Created on Dec 29, 2011

@author: manuel
'''

import csv
import codecs
from operator import itemgetter, attrgetter
import time
import commands
import re
import cStringIO

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
            WAEHRUNG = {"USD": u"$", "EUR": u"\u20ac", "GBP": u"\u00a3"}
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
        return "<Txn: %s (%s) \"%s\" %s %s - %s>" % (self.getTypeCharacter(), self.id, self.name, self.waehrung, self.brutto, self.description)
    
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
        
    def getCurrencyConversion(self):
        conversions = [ x for x in self.getReferrers() if x.isConversion() ]
        _from = [ x for x in conversions if x.waehrung != "$" ]
        _to = [ x for x in conversions if x.waehrung == "$" ]
        if _from and _to:
            return (_from[0], _to[0])
        else:
            return (None, None)
        
    def convertCurrency(self):
        (_from, _to) = self.getCurrencyConversion()
        if _to:
            self.real_brutto = "%s %s" % (self.waehrung, self.brutto)
            self.brutto = _to.brutto
            self.currency = u"$"
            self.guthaben = _to.guthaben
        
    def getUSDValue(self):
        (_from, _to) = self.getCurrencyConversion()
        if _to:
            return _to.brutto
        else:
            return u"0.00" 
        
    def needsConversion(self):
        return self.waehrung != "$"
    
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
        regexps = ["ZERO INCH", "Boomkat", "Bleep", "buyolympia"]
        for _re in regexps:
            if re.match(_re, self.name):
                return True
        return False
    
    def toCSV(self):
        row = {}
        if self.needsConversion():
            amount = self.getUSDValue()
        else:
            amount = self.brutto
        row["date"] = time.strftime("%d.%m.%Y", self.date)
        row["name"] = self.name
        row["original"] = self.real_brutto
        if amount != "0.00":
            row["brutto"] = u"$%s" % amount
        else:
            row["brutto"] = u""
        if self.gebuehr != u"0.00":
            row["gebuehr"] = u"$%s" % self.gebuehr
        else:
            row["gebuehr"] = u""
        row["guthaben"] = u"$%s" % self.guthaben
        row["notes"] = u""
        if self.isPrivate():
            row["notes"] += u"PRIVAT"
        else:
            row["notes"] += self.description
        #if self.findEmail():
        #    row["notes"] += u" EMAIL"
            
        return row
    
    def findSimilarEmails(self):
        _created = "created:%s" % (time.strftime("%d/%m/%Y", self.date))
        def find(query):
            return "mdfind -interpret '%s %s kind:mail'" % (_created, query)
        
        _cmd = ";".join([find(x) for x in [self.name, self.receiver]]) 
        print _cmd
        _file = commands.getstatusoutput(_cmd)[1]
        files = _file.split("\n")
        _email = self.findEmail()
        if _email and files.count(_email) == 0:
            files.append(_email)
#        if files.count(_email) > 0:
#            files.remove(_email)
        return files
    
    def openSimilarEmails(self):
        for email in self.findSimilarEmails():
            commands.getstatusoutput("open \"%s\"" % email)
    
    def findEmail(self):
#        _cmd = "mdfind -interpret '%s' | grep IMAP | head -1" % self.id
        _cmd = "/usr/local/bin/sense -n 0.8 'mdfind -literal \"kMDItemTextContent = *%s && kMDItemContentType=com.apple.mail.elmx \"'" % self.id
        print _cmd
        _file = commands.getstatusoutput(_cmd)[1]
        if _file != '':
            print "found %s" % _file
            return _file
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
            auszug.addTxn(row)
        auszug.convertCurrency()
        return auszug
    
    def openEmails(self):
        for txn in self.sortedTxns():
            if txn.findEmail() and (not txn.isPrivate()):
                print txn
                txn.openEmail()
                raw_input("Press Enter to continue")
    
    
    def printCSV(self, path):
        """
        Foobar
        """
        f = codecs.open(path, 'w', 'utf-8')
        writer = UnicodeWriter(f)
        #        writer = csv.writer(codecs.open(path, 'w', 'utf-8'), delimiter=',', quotechar="\"")
        writer.writerow([u"Datum", u"Name", u"Brutto", u"Original", u"Gebühr", u"Kontostand", u"Notes"])
        for txn in self.sortedTxns():
            if txn.isPayment():
                row = txn.toCSV()
                if row["brutto"] != "":
                    writer.writerow([row[x] for x in ["date", "name", "brutto", "original", "gebuehr", "guthaben", "notes"]])

auszug = Auszug.readCSV("/Users/manuel/Downloads/Herunterladen.csv")
auszug.printCSV("/tmp/out.csv")
print "printed CSV out to /tmp/out.csv\n"
for txn in auszug.sortedTxns():
    email = txn.findEmail()
    emails = txn.findSimilarEmails()
    print "%s: %s, %s\n" % (txn.id, email, emails)

def openSpotlight(query):
    applescript = '''on run argv # we expect program arguments
    tell application "Finder"
        activate # focus Finder
        tell application "System Events"
            keystroke "f" using command down # press Cmd-F
            keystroke (item 1 of argv) # enter the program argument into search box
            key code 36 # press enter
            key code 48 # press tab
            keystroke " " # press space
        end tell
    end tell
end run
'''
    _cmd = "osascript " + " ".join([ "-e '%s'" % x for x in applescript.split('\n')]) + " " + query
    commands.getstatusoutput(_cmd)
    

