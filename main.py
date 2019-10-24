import shufersal

    # filename = shufersal.downloadStorePricesFull(i)
    # shufersal.loadXMLtoDb(filename, "prices.db", "shufersal")
filename = shufersal.downloadStores()
shufersal.loadStoresToDb(filename, "prices.db","stores")