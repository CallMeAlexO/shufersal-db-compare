from datetime import datetime
from typing import NewType

import requests
import lxml.html as lh
import gzip
import xml.etree.ElementTree as ET
import guid
import os
import sqlite3

CATEGORIES = {"prices" : 1, "pricesFull": 2, "promos": 3, "promosFull": 4, "stores": 5, "all": 0}
DATABASE_STRUCTURE_STORE = "ChainId int, StoreId int, PriceUpdateDate datetime, ItemCode int, ItemType int, ItemName text, " \
                     "ManufacturerName text, ManufactureCountry text, ManufacturerItemDescription text, UnitQty text, " \
                     "Quantity real, bIsWeighted int, UnitOfMeasure text, QtyInPackage int, ItemPrice real, " \
                     "UnitOfMeasurePrice real, AllowDiscount int, ItemStatus int"
DATABASE_STRUCTURE_STORES = "ChainId int, SUBCHAINID int, STOREID int, BIKORETNO int, STORETYPE int, CHAINNAME TEXT, SUBCHAINNAME TEXT, STORENAME TEXT, ADDRESS TEXT, CITY TEXT, ZIPCODE TEXT"
COLUMNS = ["ChainId", "StoreId", "PriceUpdateDate", "ItemCode", "ItemType", "ItemName", "ManufacturerName",
           "ManufactureCountry", "ManufacturerItemDescription", "UnitQty", "Quantity", "bIsWeighted", "UnitOfMeasure",
           "QtyInPackage", "ItemPrice", "UnitOfMeasurePrice", "AllowDiscount", "ItemStatus"]
CHAIN_ID = 7290027600007

def downloadStorePricesFull(storeId):
    requestUrl = "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID={}&storeId={}"
    page = requests.get(requestUrl.format(CATEGORIES["pricesFull"],storeId))
    doc = lh.fromstring(page.content)
    tr_elements = doc.xpath("//tr[contains(@class, 'webgrid-row-style')]//a")
    if len(tr_elements) < 1: return
    name = str(guid.guid.uuid4())
    try:
        name = tr_elements[0].getparent().getparent().getchildren()[6].text + ".xml.gz"
    except:
        pass
    today = datetime.today().strftime("%Y%m%d")
    if os.path.isfile(name): return name
    if today not in name: return
    downloadLink = tr_elements[0].attrib["href"]
    data = requests.get(downloadLink).content
    with open(name, "wb") as f:
        f.write(data)
    return name

def loadXMLtoDb(name, dbname, table):

    if name is None: return
    if os.path.isfile(name):
        STORE_ID = name.split("-")[1]
    else:
        return
    # Read the file into a struct
    input = gzip.open(name, 'r')
    tree = ET.parse(input)
    products = []

    # Convert XML structure to DB structure
    items = [i for i in tree.getroot().find("Items")]
    for item in items:
        itemData = tuple([CHAIN_ID,STORE_ID]+[c.text for c in item])
        products.append(itemData)

    # If the table does not exist, create it
    conn = sqlite3.connect(dbname)
    conn.execute("CREATE TABLE IF NOT EXISTS {} (".format(table)+DATABASE_STRUCTURE_STORE+", PRIMARY KEY (ChainId,StoreId,ItemCode)) ")
    c = conn.cursor()

    # Insert into database
    sql = 'INSERT OR REPLACE INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(table)
    c.executemany(sql, list(products))
    c.close()

    # Save changes
    conn.commit()

def loadStoresToDb(name, dbname, table):
    if name is None: return
    input = gzip.open(name, 'r')
    tree = ET.parse(input)
    products = []
    items = [i for i in tree.findall(".//STORES[1]")[0].getchildren()]
    for item in items:
        itemData = tuple([CHAIN_ID]+[c.text for c in item])
        products.append(itemData)

    conn = sqlite3.connect(dbname)
    conn.execute("CREATE TABLE IF NOT EXISTS {} (".format(table)+DATABASE_STRUCTURE_STORES+", PRIMARY KEY (SUBCHAINID,STOREID,ZIPCODE)) ")
    c = conn.cursor()

    # Insert into database
    sql = 'INSERT OR REPLACE INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?)'.format(table)
    c.executemany(sql, list(products))
    c.close()

    conn.commit()

def downloadStores():
    requestUrl = "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID={}&storeId={}"
    page = requests.get(requestUrl.format(CATEGORIES["stores"],0))
    doc = lh.fromstring(page.content)
    tr_elements = doc.xpath("//tr[contains(@class, 'webgrid-row-style')]//a")
    if len(tr_elements) < 1: return
    name = str(guid.guid.uuid4())
    try:
        name = tr_elements[0].getparent().getparent().getchildren()[6].text + ".xml.gz"
    except:
        pass
    today = datetime.today().strftime("%Y%m%d")
    if os.path.isfile(name): return name
    if today not in name: return
    downloadLink = tr_elements[0].attrib["href"]
    data = requests.get(downloadLink).content
    with open(name, "wb") as f:
        f.write(data)
    return name