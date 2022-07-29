# Web app adapted from https://python-adv-web-apps.readthedocs.io/en/latest/flask_forms.html

from datetime import datetime

import pandas as pd
import sqlalchemy as db
from flask import Flask, render_template
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from sqlalchemy import Table, MetaData, desc
from sqlalchemy.sql import select, insert
from web3 import Web3
from wtforms import StringField, SubmitField, RadioField, HiddenField
from wtforms.validators import DataRequired

import config as cfg

app = Flask(__name__)

# Flask-WTF requires an enryption key - the string can be anything
app.config['SECRET_KEY'] = cfg.config['FLASK_SECRET']

# Flask-Bootstrap requires this line
Bootstrap(app)

try:
    SQLITE_DB = 'sqlite:///../indexer.sqlite?check_same_thread=False'
    engine = db.create_engine(SQLITE_DB)
    con = engine.connect()
    metadata = MetaData()
    con.execute('SELECT * FROM contracts')
except db.exc.OperationalError:
    SQLITE_DB = 'sqlite:///indexer.sqlite?check_same_thread=False'
    engine = db.create_engine(SQLITE_DB)
    con = engine.connect()
    metadata = MetaData()

RPC_URL = 'https://evm.cronos.org'
w3 = Web3(Web3.HTTPProvider(RPC_URL))


# with Flask-WTF, each web form is represented by a class
# "NameForm" can change; "(FlaskForm)" cannot
# see the route for "/" and "index.html" to see how this is used
class NameForm(FlaskForm):
    name = StringField('Check Contract Address', validators=[DataRequired()])
    # network =
    submit = SubmitField('Submit')


class VoteForm(FlaskForm):
    comment = StringField('Comment for contract', validators=[DataRequired()])
    vote = RadioField(choices=['Good', 'Bad'], validators=[DataRequired()])
    contract = HiddenField()
    submit = SubmitField('Submit')


def get_contract_details(contract_addr):
    try:
        contracts_table = Table('contracts', metadata, autoload=True, autoload_with=engine)
        contract_addr = Web3.toChecksumAddress(contract_addr)
        res = con.execute(select(contracts_table).where(contracts_table.c.address == contract_addr))
        details = res.all()[0]
        created_block, creator_address, num_tx = details[1], details[2], details[3]
        return contract_addr, created_block, creator_address, num_tx
    except:
        return None


def get_latest_block_in_db():
    blocks_table = Table('blocks', metadata, autoload=True, autoload_with=engine)
    res = con.execute(select([blocks_table]).order_by(desc(blocks_table.c.number))).fetchone()[0]
    return res


def add_vote(contract, vote, comment):
    votes_table = Table('votes', metadata, autoload=True, autoload_with=engine)
    res = con.execute(
        insert(votes_table).values(contract=contract, vote=vote, comment=comment, datetime=datetime.now()))
    return res


def get_votes(contract_addr):
    votes_table = Table('votes', metadata, autoload=True, autoload_with=engine)
    res = con.execute(
        select(votes_table).where(votes_table.c.contract == contract_addr)).fetchall()
    if len(res) == 0:
        return 0, 0
    reslist = []
    for val in res:
        reslist.append(val[2])
    good = reslist.count('Good')
    bad = reslist.count('Bad')
    return good, bad


@app.route('/', methods=['GET', 'POST'])
def index():
    form = NameForm()
    form2 = VoteForm()
    db_block = get_latest_block_in_db()
    latest_block = w3.eth.get_block('latest')
    message2 = f'DB caught up to {db_block}, {latest_block.number - db_block} behind the latest block'
    message = ""
    if form.validate_on_submit():
        contract_addr = form.name.data.strip()
        details = get_contract_details(contract_addr)
        try:
            good_vote, bad_vote = get_votes(contract_addr)
        except:
            pass
        if details is not None:
            quantile, points = get_quantile(contract_addr)

            if good_vote - bad_vote >= 10:
                points += 3
            elif bad_vote - good_vote >= 10:
                points -= 3
            message = f'{details[0]} created at block {details[1]}.\n\nTotal number of transactions: {details[3]}\n\nThis contract is in the {quantile} quantile of contracts with more than 100 transactions.'
            if good_vote == 0 and bad_vote == 0:
                votes_msg = f'There are no votes for this contract yet.\n\nIt has a reputation score of {points} points.'
            else:
                votes_msg = f'There are {good_vote} Good votes and {bad_vote} Bad votes for this contract.\n\nIt has a reputation score of {points} points.'
            text = message.split('\n')
            text2 = votes_msg.split('\n')
            form2.contract.data = contract_addr
            return render_template('contract.html', message=message, message2=message2, text=text, form=form2,
                                   text2=text2)
        else:
            message = "Not found"
    if form2.validate_on_submit():
        if form2.submit.data:
            comment = form2.comment.data
            vote = form2.vote.data
            contract_addr = form2.contract.data
            res = add_vote(contract_addr, vote, comment)
            return render_template('success.html', message=f'Submitted! Thank you')

    return render_template('index.html', form=form, message=message, message2=message2)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


def get_quantile(contract_addr):
    contracts_table = Table('contracts', metadata, autoload=True, autoload_with=engine)
    contract_addr = Web3.toChecksumAddress(contract_addr)
    df = pd.read_sql_query(select(contracts_table), engine)
    # Get only contracts with > 100 tx
    df2 = df[df['num_tx'] > 100]
    # Get index of corresponding addr
    addr_index = df2.index[df2['address'] == contract_addr].to_list()[0]
    # Get rank of values in truncated list
    ranked_df2 = df2.rank(pct=True)
    # Get rank of contract
    rank = ranked_df2.loc[addr_index].num_tx
    if 1 - rank < 0.01:
        points = 5
    elif 1 - rank < 0.05:
        points = 4
    elif 1 - rank < 0.1:
        points = 3
    elif 1 - rank < 0.2:
        points = 2
    elif 1 - rank < 0.5:
        points = 1
    rank = f'{ranked_df2.loc[index].num_tx:.2%}'
    return rank, points


# Below function adapted from https://stackoverflow.com/a/54317197/15104061
def cdf_series(df):
    stats_df = df.groupby('num_tx') \
        ['num_tx'] \
        .agg('count') \
        .pipe(pd.DataFrame) \
        .rename(columns={'num_tx': 'frequency'})

    # PDF
    stats_df['pdf'] = stats_df['frequency'] / sum(stats_df['frequency'])

    # CDF
    stats_df['cdf'] = stats_df['pdf'].cumsum()
    stats_df = stats_df.reset_index()
    stats_df


# keep this as is
if __name__ == '__main__':
    app.run(debug=True)
