# encoding: utf-8
import sys
import urlparse
from psycopg2.pool import SimpleConnectionPool
from decimal import Decimal

def _request_db(conn, query="", all=True):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall() if all else cur.fetchone()
    cur.close()
    return result

def question1(db_name):
    """
    What is the total amount of money for the goods which are planned to be
    sold?
    """
    result = _request_db(conn,
                         "select sum(amount_total) "
                         "from sale_order "
                         "where state != 'draft'",
                         all=False)
    return 'Total amount: {}'.format(result[0])


def question2(conn):
    """
    What is the total amount of money for the sale orders which was made by
    each system user? (except „sale_order“ tables entries with „state“ field
    „draft“ value)
    """
    result = _request_db(conn,
                         "select t2.name, sum(t1.amount_total) "
                         "from sale_order as t1 left join res_users as t2 on t1.user_id=t2.id "
                         "where t1.state != 'draft' group by t2.name")
    results = ['{} -> {}'.format(res[0], res[1]) for res in result]
    return '\n\r'.join(results)


def question3(conn):
    """
    Top 3 company clients which made the biggest total amount for the sale
    orders?
    """
    result = _request_db(conn,
                         "select a2.name, sum(a1.amount_total) as total "
                         "from sale_order as a1 join res_partner as a2 on a1.partner_id=a2.id "
                         "group by a2.name order by total desc "
                         "limit 3")
    results = ['{} -> {}'.format(res[0], res[1]) for res in result]
    return '\n\r'.join(results)

def question4(conn):
    """
    Which sales orders associated with the account are fully paid up and which
    one of them is the biggest one?
    """
    result = _request_db(conn,
                         "select t4.name, t1.amount_total "
                         "from sale_order as t1 join sale_order_invoice_rel as t2 on t1.id=t2.order_id join res_partner as t4 on t1.partner_id=t4.id "
                         "where t2.invoice_id in (select t3.id from account_invoice as t3 where t3.state='paid') "
                         "order by t1.amount_total desc ")
    results = ['{} -> {}'.format(res[0], res[1]) for res in result]
    return '\n\r'.join(results)

def question5(conn):
    """
    What will be the company's "profit" in 2011:
    if the outstanding balances of account will be written off to losses;
    if the outstanding balances of account will be paid 35% and the rest part will
    be written of to losses
    """
    paid = _request_db(conn,
                       "select sum(amount_total) "
                       "from account_invoice "
                       "where date_invoice>='2011-01-01' and date_invoice<='2011-12-31' "
                       "and state='paid'",
                       all=False)
    pending = _request_db(conn, "select sum(amount_total) "
                       "from account_invoice "
                       "where date_invoice>='2011-01-01' and date_invoice<='2011-12-31' "
                       "and not state in ('draft', 'paid')",
                       all=False)
    # First balance
    balance1 = paid[0] - pending[0]
    # Second balance
    balance2 = paid[0] + pending[0] * Decimal(0.35) - pending[0] * Decimal(0.65)
    results = ['#1: {}'.format(balance1), '#2: {:.2f}'.format(balance2)]
    return '\n\r'.join(results)


def url_to_dsn(url):
    parsed = urlparse.urlparse(url)
    dbname = parsed.path[1:] # /foobar
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    if port is None:
        port = '5432' # postgres default port
    dsn = "dbname={} host={} port={}".format(dbname, host, port)
    if user:
        dsn += ' username={}'.format(user)
    if password:
        dsn += ' password={}'.format(password)
    return dsn

if __name__ == "__main__":
    # Getting dsn from console arguments
    # postgres://user:password@localhost:5432/test_erp
    if 'postgres' not in urlparse.uses_netloc:
        # Teach urlparse about postgres:// URLs.
        urlparse.uses_netloc.append('postgres')
    if len(sys.argv) > 1:
        conn_string = url_to_dsn(sys.argv[1])
    else:
        conn_string = url_to_dsn("postgres://localhost:5432/test_erp")

    # creating pool
    pool = SimpleConnectionPool(1, 5, dsn=conn_string)
    for i in xrange(1,6):
        print "Question {}:\n\r{}".format(i, getattr(sys.modules[__name__], 'question{}'.format(i)).__doc__)
        conn = pool.getconn()
        print getattr(sys.modules[__name__], 'question{}'.format(i))(conn)
        pool.putconn(conn)
        print "="*20
    pool.closeall()