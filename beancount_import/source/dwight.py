"""Splitwise transaction source.

This imports transactions from Dwight.com CSV export files.

Data format
===========

To use, first download Dwight.com transactions and balance information as CSV
files stored on the filesystem.  The easiest way to download data from Dwight in
the requisite format is to use the finance_dl.dwight module.

You might have a directory structure like:

    financial/
      data/
        dwight/
          dwight.csv

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Dwight source:

    dict(module='beancount_import.source.dwight',
         directory=os.path.join(journal_dir, 'data', 'dwight', 'dwight.csv'),
         balances_directory=os.path.join(journal_dir, 'data', 'dwight'),
    )

where `journal_dir` refers to the financial/ directory.  Specifying the
`balances_directory` key is optional.  If not specified, balance information
won't be imported.

Associating Dwight accounts with Beancount accounts
=================================================

This data source only imports transactions from accounts known to Dwight with
which a Beancount account has been explicitly associated using the `dwight_id`
metadata field of the account open directive.  The `dwight_id` corresponds to the
"Account Name" field in the CSV file.  As this "Account Name" excludes the
institution name, it is possible that the "Account Name" values are not unique,
in which case you can change them using the Dwight.com web interface, before
re-downloading the transactions.  For example:

    1900-01-01 open Liabilities:Credit-Card  USD
      dwight_id: "My Credit Card"

    1900-01-01 open Assets:Checking  USD
      dwight_id: "My Checking"

    1900-01-01 open Liabilities:Amazon-Store-Card  USD
      dwight_id: "Amazon Store Card"

Imported transaction format:
============================

Each row in the transactions CSV file corresponds to a single imported
transaction of the form:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
      Expenses:FIXME            2.45 USD

Transaction identification
--------------------------

The `date` and `source_desc` metadata fields (along with the account and amount)
associate postings in the journal with corresponding rows in the transactions
CSV file.  These fields correspond to the "Date" and "Original Description"
fields in the transactions CSV file, respectively.  It is possible for multiple
real transactions to have an identical combination of account, amount, "Date",
and "Original Description" (corresponding to multiple identical rows in the
transactions CSV file), but that is handled appropriately: this data source will
simply generate a separate transaction for each such row.

The transactions CSV export format provided by Dwight and consumed by this data
source does not include a unique transaction identifier, except in the case that
Dwight has (erroneously) included a unique identifier provided by the financial
institution in the "Original Description" field.  Internally, Dwight does expose a
unique transaction identifier through the undocumented JSON API, but this data
source does not attempt to use them.

Unknown account prediction
--------------------------

The `source_desc` metadata field provides features for predicting the unknown
account.  The transactions CSV format includes additional "Description" and
"Category" fields that are synthesized by Dwight from the original data, and
potentially provide some information that could be useful for predicting the
unknown account.  However, this data source does not rely on those fields, as
they are not stable (meaning they may change on a subsequent download).

Handling duplicate transactions
-------------------------------

Dwight sometimes incorrectly creates duplicate transactions.  This is different,
but indistinguishable, from the case of two real transactions with the same
account, amount, date, and description.  After verifying that it is really a
duplicate, there are two ways you can deal with this:

 - You can manually add the duplicate description as an additional
   `source_desc1` (or `source_desc2` or `source_desc3`) metadata field to the
   existing posting to which it corresponds.  This is likely to be the easiest
   method.  For example:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
        source_desc1: "STARBUCKS STORE 12345"
      Expenses:Coffee            2.45 USD


 - If you are using the finance_dl.dwight module to download data, you can mark
   the transaction as a duplicate through the Dwight.com web interface, and then
   re-download the transactions.  The finance_dl.dwight module automatically
   excludes transactions that have been marked as duplicates.
"""

from typing import List, Union, Optional, Set
import csv
import datetime
import collections
import re
import os

from beancount.core.data import Transaction, Posting, Balance, EMPTY_SET
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from . import description_based_source
from . import ImportResult, SourceResults
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

# account may be either the dwight_id or the journal account name
DwightEntry = collections.namedtuple(
    'DwightEntry',
    ['account', 'date', 'amount', 'source_desc', 'filename', 'line'])
RawBalance = collections.namedtuple(
    'RawBalance', ['account', 'date', 'amount', 'filename', 'line'])


def get_info(raw_entry: Union[DwightEntry, RawBalance]) -> dict:
    return dict(
        type='text/csv',
        filename=raw_entry.filename,
        line=raw_entry.line,
    )


dwight_date_format = '%Y-%m-%d'


def load_transactions(filename: str, currency: str = 'USD') -> List[DwightEntry]:
    expected_field_names = [
        'Date', 'Description', 'Category', 'Cost',
        'Currency', 'Zhuohan Li', 'Zhanghao Wu'
    ]

    try:
        entries = []
        filename = os.path.abspath(filename)
        with open(filename, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames != expected_field_names:
                raise RuntimeError(
                    'Actual field names %r != expected field names %r' %
                    (reader.fieldnames, expected_field_names))
            for line_i, row in enumerate(reader):
                account = "Zhuohan Li"
                sgn = row['Zhanghao Wu'][0]
                number = D(row['Zhanghao Wu']) if sgn != '-' else -D(row['Zhanghao Wu'][1:])
                if number == ZERO or row['Description'] == 'Total balance':
                    # Skip zero-dollar transactions.
                    # Some banks produce these, e.g. for an annual fee that is waived.
                    continue

                try:
                    date = datetime.datetime.strptime(row['Date'],
                                                      dwight_date_format).date()
                except Exception as e:
                    raise RuntimeError('Invalid date: %r' % row['Date']) from e

                entries.append(
                    DwightEntry(
                        account=account,
                        date=date,
                        source_desc=row['Description'],
                        amount=Amount(number=number, currency=currency),
                        filename=filename,
                        line=line_i + 1))
        entries.reverse()
        entries.sort(key=lambda x: x.date)  # sort by date
        return entries

    except Exception as e:
        raise RuntimeError('CSV file has incorrect format', filename) from e


def load_balances(filename: str) -> List[RawBalance]:
    expected_field_names = [
        'Date', 'Description', 'Category', 'Cost',
        'Currency', 'Zhuohan Li', 'Zhanghao Wu'
    ]
    balances = []
    filename = os.path.abspath(filename)
    with open(filename, 'r', encoding='utf-8', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames != expected_field_names:
            raise RuntimeError(
                'Actual field names %r != expected field names %r' %
                (reader.fieldnames, expected_field_names))
        account = "Zhuohan Li"

        for line_i, row in enumerate(reader):
            if row['Description'] != 'Total balance': continue
            try:
                date = datetime.datetime.strptime(row['Date'],
                                                    dwight_date_format).date()
            except Exception as e:
                raise RuntimeError('Invalid date: %r' % row['Date']) from e
            sgn = row['Zhanghao Wu'][0]
            number = D(row['Zhanghao Wu']) if sgn != '-' else -D(row['Zhanghao Wu'][1:])
            balances.append(
                RawBalance(
                    account=account,
                    date=date,
                    amount=Amount(number, row['Currency']),
                    filename=filename,
                    line=line_i + 1))
        return balances


def _get_key_from_posting(entry: Transaction, posting: Posting,
                          source_postings: List[Posting], source_desc: str,
                          posting_date: datetime.date):
    del entry
    del source_postings
    return (posting.account, posting_date, posting.units, source_desc)


def _get_key_from_csv_entry(x: DwightEntry):
    return (x.account, x.date, x.amount, x.source_desc)


def _make_import_result(dwight_entry: DwightEntry) -> ImportResult:
    transaction = Transaction(
        meta=None,
        date=dwight_entry.date,
        flag=FLAG_OKAY,
        payee=None,
        narration=dwight_entry.source_desc,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[
            Posting(
                account=dwight_entry.account,
                units=dwight_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=collections.OrderedDict(
                    source_desc=dwight_entry.source_desc,
                    date=dwight_entry.date,
                )),
            Posting(
                account=FIXME_ACCOUNT,
                units=-dwight_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=None,
            ),
        ])
    return ImportResult(
        date=dwight_entry.date, info=get_info(dwight_entry), entries=[transaction])


class DwightSource(description_based_source.DescriptionBasedSource):
    def __init__(self,
                 filename: str,
                 balances_directory: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.filename = filename
        self.balances_directory = balances_directory

        # In these entries, account refers to the dwight_id, not the journal account.
        self.log_status('dwight: loading %s' % filename)
        self.dwight_entries = load_transactions(filename)

        self.balances = [] # type: List[RawBalance]
        if balances_directory:
            self.log_status('dwight: loading %s' % balances_directory)
            self.balances.extend(load_balances(balances_directory))

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        account_to_dwight_id, dwight_id_to_account = description_based_source.get_account_mapping(
            journal.accounts, 'dwight_id')
        missing_accounts = set()  # type: Set[str]

        def get_converted_dwight_entries(entries):
            for raw_dwight_entry in entries:
                account = dwight_id_to_account.get(raw_dwight_entry.account)
                if not account:
                    missing_accounts.add(raw_dwight_entry.account)
                    continue
                match_entry = raw_dwight_entry._replace(account=account)
                yield match_entry

        description_based_source.get_pending_and_invalid_entries(
            raw_entries=get_converted_dwight_entries(self.dwight_entries),
            journal_entries=journal.all_entries,
            account_set=account_to_dwight_id.keys(),
            get_key_from_posting=_get_key_from_posting,
            get_key_from_raw_entry=_get_key_from_csv_entry,
            make_import_result=_make_import_result,
            results=results)

        for dwight_account in missing_accounts:
            results.add_warning(
                'No Beancount account associated with Dwight account %r.' %
                (dwight_account, ))

        for raw_balance in get_converted_dwight_entries(self.balances):
            date = raw_balance.date + datetime.timedelta(days=1)
            results.add_pending_entry(
                ImportResult(
                    date=date,
                    info=get_info(raw_balance),
                    entries=[
                        Balance(
                            account=raw_balance.account,
                            date=date,
                            meta=None,
                            amount=raw_balance.amount,
                            tolerance=None,
                            diff_amount=None)
                    ]))

    @property
    def name(self):
        return 'dwight'


def load(spec, log_status):
    return DwightSource(log_status=log_status, **spec)
