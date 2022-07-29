import asyncio
import logging

import sqlalchemy as db
from sqlalchemy import Table, MetaData, Column, String, Integer, Boolean, ForeignKey, desc, DateTime
from sqlalchemy.sql import select
from web3 import Web3

import config as cfg

RPC_URL = cfg.config['RPC_URL']
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# create logger
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('main.log')
fh.setLevel(logging.INFO)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

try:
    SQLITE_DB = 'sqlite:///indexer.sqlite'
    engine = db.create_engine(SQLITE_DB)
    con = engine.connect()
    metadata = MetaData()
except:
    logger.error('Error when opening API', exc_info=True)


async def main():
    try:
        blocks_table = Table('blocks', metadata, autoload=True, autoload_with=engine)
        # Get unprocessed blocks
        res = con.execute(
            select([blocks_table]).where(blocks_table.c.processed == False).where(blocks_table.c.num_txs != 0))
        unprocessed_blocks = res.all()
        # Get list of unprocessed blocks
        blocks = []
        for block in unprocessed_blocks:
            blocks.append(block[0])
        logger.info('Unprocessed blocks')
        logger.info(blocks)
        for i in blocks:
            processBlock(i)
        # Get next block to process
        stmt = select([blocks_table]).order_by(desc('number')).limit(1)
        res = con.execute(stmt)
        try:
            next_block = res.fetchone()[0] + 1
        except TypeError:
            next_block = 0
        latest_block = (w3.eth.get_block('latest'))
        for i in range(next_block, latest_block.number):
            processBlock(i)
    except KeyboardInterrupt:
        return


def processBlock(block):
    try:
        blocks_table = Table('blocks', metadata, autoload=True, autoload_with=engine)
        count = 0
        block = (w3.eth.get_block(block))
        block_tx_count = len(block.transactions)
        if blockInDb(block.number):
            pass
        else:
            addBlock(block.number, block_tx_count)
        if block_tx_count > 0:
            for transaction in block.transactions:
                transaction_details = w3.eth.getTransaction(transaction)
                to_address = transaction_details.to
                # Check if contract creation
                if transaction_details.to is None:
                    receipt = w3.eth.getTransactionReceipt(transaction)
                    to_address = receipt.contractAddress
                    addContract(to_address, block.number, transaction_details['from'])
                # Check if contract in DB
                if contractInDb(to_address):
                    pass
                else:
                    # If not, check if is contract
                    code = w3.eth.get_code(to_address)
                    if code.hex() == '0x':
                        pass
                    else:
                        addContract(to_address, block.number, to_address)
                        # Add to db
                # Index TX
                addTx(transaction.hex(), block.number, transaction_details['from'], to_address)
                count += 1
            if count == block_tx_count:
                # Check block tx count in db. If match indexed then mark done
                con.execute(
                    blocks_table.update().where(blocks_table.c.number == block.number).values(processed=True))
        else:
            # Check block tx count in db. If 0 tx then mark done
            con.execute(
                blocks_table.update().where(blocks_table.c.number == block.number).values(processed=True))
            logger.debug(f'Block {block.number} no tx')
    except:
        logger.error('Error', exc_info=True)


def contractInDb(contract_address):
    contracts_table = Table('contracts', metadata, autoload=True, autoload_with=engine)
    stmt = select([contracts_table]).where(contracts_table.c.address == contract_address)
    result = con.execute(stmt).fetchall()
    if len(result) == 0:
        return False
    return True


def blockInDb(block):
    blocks_table = Table('blocks', metadata, autoload=True, autoload_with=engine)
    stmt = select([blocks_table]).where(blocks_table.c.number == block)
    result = con.execute(stmt).fetchall()
    if len(result) == 0:
        return False
    return True


def addTx(txid, block, from_address, to_address):
    tx_table = Table('transactions', metadata, autoload=True, autoload_with=engine)
    contracts_table = Table('contracts', metadata, autoload=True, autoload_with=engine)
    con.execute(
        tx_table.insert().values(txid=txid, block=block, from_address=from_address, to_address=to_address).prefix_with(
            "OR IGNORE"))
    con.execute(
        contracts_table.update(contracts_table.c.address == to_address).values(num_tx=contracts_table.c.num_tx + 1))
    logger.debug(f'TX {txid} added to DB')
    return


def addBlock(number, num_txs):
    blocks_table = Table('blocks', metadata, autoload=True, autoload_with=engine)
    con.execute(blocks_table.insert().values(number=number, num_txs=num_txs).prefix_with("OR IGNORE"))
    logger.debug(f'Block {number} added to DB')
    return


def addContract(address, block, creator_address):
    contracts_table = Table('contracts', metadata, autoload=True, autoload_with=engine)
    con.execute(contracts_table.insert().values(address=address, created_block=block,
                                                creator_address=creator_address, num_tx=1).prefix_with("OR IGNORE"))
    logger.debug(f'Contract {address} added to DB')
    return


async def check_sql_engine(metadata=metadata):
    # Create a table with the appropriate Columns
    Table('blocks', metadata,
          Column('number', Integer, primary_key=True, nullable=False, autoincrement=False),
          Column('num_txs', Integer),
          Column('processed', Boolean, default=False),
          )
    Table('contracts', metadata,
          Column('address', String, primary_key=True, nullable=False),
          Column('created_block', Integer, ForeignKey('blocks.number')),
          Column('creator_address', String),
          Column('num_tx', Integer)
          )
    Table('transactions', metadata,
          Column('txid', String, primary_key=True, nullable=False),
          Column('block', Integer, ForeignKey('blocks.number')),
          Column('from_address', String),
          Column('to_address', String),
          )
    Table('votes', metadata,
          Column('id', Integer, primary_key=True, autoincrement=True),
          Column('contract', Integer, ForeignKey('contracts.address')),
          Column('vote', String),
          Column('comment', String),
          Column('datetime', DateTime),
          )

    # Implement the creation
    metadata.create_all(engine)
    version = con.execute('PRAGMA user_version').fetchall()[0][0]
    if version == 0:
        try:
            con.execute('ALTER TABLE contracts ADD COLUMN num_tx INTEGER')
            con.execute('UPDATE contracts SET num_tx = 1')
        except:
            pass
        con.execute('PRAGMA user_version = 1')
    if version == 1:
        try:
            con.execute(
                'CREATE TABLE votes (id INTEGER NOT NULL, 	contract INTEGER, 	vote VARCHAR, 	comment VARCHAR, 	datetime DATETIME, 	PRIMARY KEY (id), 	FOREIGN KEY(contract) REFERENCES contracts (address))')
        except:
            pass
        con.execute('PRAGMA user_version = 2')


loop = asyncio.get_event_loop()
asyncio.run(check_sql_engine())
loop.run_until_complete(main())
