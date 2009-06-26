import sys
import base64
import getpass
import datetime
import copy
import os
import os.path
from ConfigParser import SafeConfigParser
import logging

from lxml import etree
from lxml.builder import ElementMaker, E

# Only needed with test code below.
#from httplib import HTTPConnection

# Using M2Crypto on the advice of Wesabe.  Supposedly does certificate
# validation (hopefully out of the box).
from M2Crypto.httpslib import HTTPSConnection

# The OFX schema seems broken, or at least xmllint thinks it is.  The
# schema seems to say that the top OFX element needs to be in the
# following namespace, but it dies when any of its child elements are
# in a namespace.  If you want your financial software to work, leave
# the first definition of OFX_NS uncommented.  If you want xmllint to
# suceed on validating our output, uncomment the second definition.
OFX_NS = E
# OFX_NS = ElementMaker(namespace="http://ofx.net/types/2003/04",
#                       nsmap={"ofx": "http://ofx.net/types/2003/04"})

class WesabeAPI (object):
    def __init__(self, username, password):
        credentials = "%s:%s" % (username, password)
        self.encoded_credentials = base64.encodestring(credentials)[:-1]

    def request(self, path):
        # A little non-SSL test code commented out, for when you just
        # want to see what the request looks like (for example).
        #from httplib import HTTPConnection
        #conn = HTTPConnection("localhost", 4242)
        conn = HTTPSConnection("www.wesabe.com")

        headers = {'Authorization': "Basic " + self.encoded_credentials,
                   # Wesabe used to let you download OFX[2].  No
                   # longer since their site redesign.  Haven't seen
                   # movement from them on this.  The below fix was
                   # suggested, but had no effect.
                   #"Accept": "*/*; application/x-ofx; application/xml",
                   }
        conn.request("GET", path, headers=headers)
        response = conn.getresponse()
        if response.status != 200:
            print response.getheaders()
            print response.reason
            print response.msg
            raise Exception("request for %r returned status %r"
                            % (path, response.status))
        return response.read()

    def request_xml(self, path):
        return etree.fromstring(self.request(path))

def ofx_datetime(datetimeish):
    # Get it?  It's anything datetime-ish!
    return datetimeish.strftime("%Y%m%d%H%M%S")

def ofx_date(datetimeish):
    return datetimeish.strftime("%Y%m%d")

class BaseAccount (object):
    def __init__(self):
        self.transactions = []
        self.fit_ids = {}

    def add_transaction(self, xact):
        self.transactions.append(xact)

        # FITIDs on transactions are used by clients to detect
        # duplicate transactions.  Thus they need to be unique within
        # an account (or maybe a whole FI?).  Wesabe doesn't give us
        # explicit FITIDs.  We synthesize them based on sorting.
        # These FITIDs look something like the ones Wesabe gives us.
        #
        # Note that we're going out of our way to guard against the
        # case of two transactions on the same date in the same
        # account for the same amount: that's what all the sorting is
        # for (see get_fit_id_for_transaction).
        bucket_id = "%s:%s:%s" % (self.id, ofx_date(xact.date), xact.amount)
        fit_ids = self.fit_ids
        bucket = fit_ids.get(bucket_id)
        if bucket is None:
            fit_ids[bucket_id] = bucket = [xact]
        else:
            bucket.append(xact)
            bucket.sort()
        xact.fit_id_bucket_id = bucket_id
        xact.fit_id_bucket = bucket

    def get_fit_id_for_transaction(self, xact):
        place_in_bucket = xact.fit_id_bucket.index(xact)
        return "%s:%d" % (xact.fit_id_bucket_id, place_in_bucket)

    def __repr__(self):
        return ("<%s id=%r account_number=%r>"
                % (self.__class__.__name__, self.id,
                   getattr(self, "account_number", None)))
        
    def get_ofx(self):
        return self.STATEMENT_TRANSACTION_RESPONSE_ELEMENT (
            E.TRNUID("1"),
            E.STATUS(
                E.CODE("0"),
                E.SEVERITY("INFO"),
                ),
            self.STATEMENT_RESPONSE_ELEMENT(
                E.CURDEF(self.currency),
                self.get_account_aggregate(self.DIR_FROM),
                self.get_banktranlist(),
                E.LEDGERBAL(
                    E.BALAMT(self.balance),
                    E.DTASOF(ofx_datetime(self.last_uploaded_at)),
                    ),
                ),
            )

    def get_banktranlist(self):
        elem = E.BANKTRANLIST(
            E.DTSTART(ofx_date(self.oldest_transaction_timestamp)),
            E.DTEND(ofx_date(self.newest_transaction_timestamp)),
            )
        for transaction in self.transactions:
            elem.append(transaction.get_ofx())
        return elem

class BankAccount (BaseAccount):
    DIR_FROM = E.BANKACCTFROM
    DIR_TO = E.BANKACCTTO

    STATEMENT_TRANSACTION_RESPONSE_ELEMENT = E.STMTTRNRS
    STATEMENT_RESPONSE_ELEMENT = E.STMTRS

    TYPE_CHECKING = "CHECKING"
    TYPE_SAVINGS = "SAVINGS"

    def get_account_aggregate(self, direction):
        return direction(
            E.BANKID(str(self.id)),
            E.ACCTID(self.account_number),
            E.ACCTTYPE(self.account_type),
            )

class CreditCardAccount (BaseAccount):
    DIR_FROM = E.CCACCTFROM
    DIR_TO = E.CCACCTTO

    STATEMENT_TRANSACTION_RESPONSE_ELEMENT = E.CCSTMTTRNRS
    STATEMENT_RESPONSE_ELEMENT = E.CCSTMTRS

    def get_account_aggregate(self, direction):
        return direction(
            E.ACCTID(self.account_number),
            )

class Transaction (object):
    def __init__(self):
        self.transfer = None

    def __repr__(self):
        return "<Transaction guid=%.8s>" % (getattr(self, "guid", None),)

    @property
    def fitid(self):
        return self.account.get_fit_id_for_transaction(self)

    # Need to be sortable.
    def __cmp__(self, other):
        return (cmp(self.date, other.date)
                or cmp(self.amount, other.amount)
                or cmp(self.name, other.name)
                or cmp(self.memo, other.memo)
                or cmp(self.guid, other.guid))

    # __hash__ because we have __cmp__.
    def __hash__(self):
        return reduce(op.xor, [ hash(v) for v in (
            self.date,
            self.amount,
            self.name,
            self.memo,
            self.guid,
            ) ])

    def get_ofx(self):
        elem = E.STMTTRN(
            E.TRNTYPE(self.raw_type),
            E.DTPOSTED(ofx_datetime(self.date)),
            E.TRNAMT(self.amount),
            E.FITID(self.fitid),
            E.NAME(self.name),
            )
        transfer = self.transfer
        if transfer is not None:
            acct = transfer.account
            elem.append(acct.get_account_aggregate(acct.DIR_TO))
        if self.memo:
            elem.append(E.MEMO(self.memo))
        return elem

ACCOUNT_TYPE_STR_TO_CONST = {
    "Checking": BankAccount.TYPE_CHECKING,
    "Savings": BankAccount.TYPE_SAVINGS,
    }

OFX_TRANSACTION_TYPES = set((
    'CREDIT', 'DEBIT', 'INT', 'DIV', 'FEE', 'SRVCHG', 'DEP', 'ATM', 'POS',
    'XFER', 'CHECK', 'PAYMENT', 'CASH', 'DIRECTDEP', 'DIRECTDEBIT',
    'REPEATPMT', 'OTHER',
    ))

WESABE_DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

def parse_wesabe_datetime(string):
    # Handle exactly Wesabe's single ISO date/time format.
    return datetime.datetime.strptime(string, WESABE_DATETIME_FMT)

def parse_accounts(accounts_etree):
    def find_text(tag):
        return account_elem.findtext(tag).strip()

    def find_datetime(tag):
        return parse_wesabe_datetime(find_text(tag))

    accounts = {}
    for account_elem in accounts_etree.xpath("/accounts/account"):
        account_type_str = find_text("account-type")
        if account_type_str == "Credit Card":
            account = CreditCardAccount()
        else:
            account = BankAccount()
            account.account_type = ACCOUNT_TYPE_STR_TO_CONST[account_type_str]
        account.id = int(account_elem.findtext("id"))
        account.account_number = find_text("account-number")
        account.currency = find_text("currency")
        # Not converting to float since it seems like an unnecessary
        # burden (we want to regurgitate a string, not output
        # 1003.110000000001).
        account.balance = find_text("current-balance")
        account.last_uploaded_at = find_datetime("last-uploaded-at")
        account.oldest_transaction_timestamp = find_datetime("oldest-txaction")
        account.newest_transaction_timestamp = find_datetime("newest-txaction")
        accounts[account.id] = account

    return accounts

def parse_transactions(transactions_etree, accounts):
    def find_text(path):
        text = xact_elem.findtext(path)
        if text is not None:
            text = text.strip()
        return text

    transactions = {}
    open_transfers = {}
    completed_transfers = []
    for xact_elem in transactions_etree.xpath("/txactions/txaction"):
        xact = Transaction()
        xact.guid = guid = find_text("guid")
        account_id = int(find_text("account-id"))
        xact.account = account = accounts[account_id]
        # date vs. original_date?  They're all the same in my file.
        # We'll take... date?
        parsed_datetime = datetime.datetime.strptime(find_text("date"),
                                                     "%Y-%m-%d")
        xact.date = parsed_datetime.date()
        # Again, not converting to a number to avoid floating point
        # representation issues.
        xact.amount = find_text("amount")
        xact.raw_type = raw_type = find_text("raw-txntype")
        assert raw_type in OFX_TRANSACTION_TYPES, `raw_type`
        # Two options: raw-name and memo, or merchant/name and
        # raw-name + memo.  The former is more like Money/Quicken and
        # is more likely to match with what you might have to use
        # (download OFX from each bank) if Wesabe stops working...
        xact.name = find_text("raw-name")
        xact.memo = find_text("memo")
        transactions[guid] = xact
        account.add_transaction(xact)
        transfer = xact_elem.find("transfer")
        if transfer is not None:
            other_end_guid = transfer.findtext("guid").strip()
            other_end = open_transfers.pop(other_end_guid, None)
            if other_end:
                completed_transfers.append((other_end, xact))
            else:
                assert other_end_guid not in transactions
                open_transfers[guid] = xact

    if open_transfers:
        raise Exception("open transfers remain: %r" % (open_transfers))

    for left, right in completed_transfers:
        left.transfer = right
        right.transfer = left

def get_ofx_for_accounts(accounts):
    bank_accounts = []
    credit_card_accounts = []
    for acct in accounts.itervalues():
        if isinstance(acct, BankAccount):
            bank_accounts.append(acct)
        else:
            credit_card_accounts.append(acct)

    ofx = OFX_NS.OFX(
        E.SIGNONMSGSRSV1(
            E.SONRS(
                E.STATUS(
                    E.CODE("0"),
                    E.SEVERITY("INFO"),
                    ),
                E.DTSERVER(ofx_datetime(datetime.datetime.now())),
                E.LANGUAGE("ENG"),
                ),
            ),
        )

    for wrapper, accounts in ((E.BANKMSGSRSV1, bank_accounts),
                                   (E.CREDITCARDMSGSRSV1,
                                    credit_card_accounts)):
        element = wrapper()
        element.extend( acct.get_ofx() for acct in accounts )
        ofx.append(element)

    return ofx

def ask_for_credentials():
    # It's a good idea not to store credentials on disk since they
    # are highly sensitive.  If you can prompt for the user's password,
    # that's the best approach.
    print "Enter username: ",
    sys.stdout.flush()
    username = sys.stdin.readline().strip()
    #username = "hardcoded@user.name"  # if you use this, comment out above
    password = getpass.getpass("Password: ")
    return (username, password)

CONFIG_SECT = "downloader"
CONFIG_LAST_RUN = "last-run"

def get_state_config_path():
    return os.path.expanduser("~/.wesabe-downloader")

def get_state_config(config_path):
    config = SafeConfigParser()
    if os.path.exists(config_path):
        config.read([config_path])
    return config

def read_state():
    config = get_state_config(get_state_config_path())
    state = {}
    if config.has_section(CONFIG_SECT):
        state.update(config.items(CONFIG_SECT))
    return state

def write_state(state):
    path = get_state_config_path()
    config = SafeConfigParser()
    config.add_section(CONFIG_SECT)
    for key, value in state.iteritems():
        config.set(CONFIG_SECT, key, value)
    config.write(open(path, "w"))

DATE_MARGIN = datetime.timedelta(days=30)

TIMESTAMP_FMT_REQUEST = "%Y%m%d"

def main(argv):
    output_path = argv[1]
    username, password = ask_for_credentials()
    wesabe = WesabeAPI(username, password)
    accounts_xml = wesabe.request_xml("/accounts.xml")
    accounts = parse_accounts(accounts_xml)
    state = read_state()
    # Instead of last run, could look at last-uploaded-at?
    last_run_str = state.get(CONFIG_LAST_RUN)
    if last_run_str:
        last_run = parse_wesabe_datetime(last_run_str)
        start_date = (last_run - DATE_MARGIN)
    else:
        # datetime.datetime.min can't be stftime'd.
        start_date = datetime.datetime(1900, 1, 1)
    start_date = start_date.strftime(TIMESTAMP_FMT_REQUEST)
    now = datetime.datetime.now()
    # Look into the future!
    end_date = (now + DATE_MARGIN).strftime(TIMESTAMP_FMT_REQUEST)
    logging.info("retrieving transactions between %s and %s",
                 start_date, end_date)
    path = ("/transactions.xml?start_date=%s&end_date=%s"
            % (start_date, end_date))
    transactions_xml = wesabe.request_xml(path)
    parse_transactions(transactions_xml, accounts)
    output = open(output_path, "w")
    # ElementTree doesn't have a way to write out PIs as siblings to
    # the document root element.
    output.write("""<?xml version="1.0" ?>
<?OFX OFXHEADER="200" VERSION="211" SECURITY="NONE" OLDFILEUID="NONE"
      NEWFILEUID="NONE" ?>
""")
    ofx_doc = etree.ElementTree(get_ofx_for_accounts(accounts))
    ofx_doc.write(output, pretty_print=True)
    output.close()
    state[CONFIG_LAST_RUN] = now.strftime(WESABE_DATETIME_FMT)
    write_state(state)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    import sys
    main(sys.argv)
